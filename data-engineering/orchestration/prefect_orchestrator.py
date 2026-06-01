from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import mysql.connector
from prefect import flow, get_run_logger, task

from config.db_config import DB_CONFIG
from config.env_loader import load_project_env
from extract.portal_exporter import download_daily_reports_from_portal
from pipelines import run_coverage_pipeline, run_data_dump_pipeline


SOURCE_MODES = {"local", "portal"}
UserLogger = Callable[[object], None]
_active_user_logger: UserLogger | None = None


def _set_active_user_logger(logger: UserLogger | None) -> UserLogger | None:
    global _active_user_logger
    previous = _active_user_logger
    _active_user_logger = logger
    return previous


def _emit(message: object, level: str = "info") -> None:
    text = str(message).strip()
    if not text:
        return

    try:
        prefect_logger = get_run_logger()
        log_method = getattr(prefect_logger, level, prefect_logger.info)
        log_method("%s", text)
    except Exception:
        prefect_logger = None

    user_logger = _active_user_logger
    if user_logger is not None and user_logger is not print:
        try:
            user_logger(text)
        except Exception:
            pass
    elif prefect_logger is None:
        print(text)


def _pipeline_logger(message: object) -> None:
    _emit(message)


def _to_iso(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return value


def _date_from_iso(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _resolve_existing_file(file_path: str, label: str) -> str:
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"{label} file not found: {path}")
    if not path.is_file():
        raise ValueError(f"{label} path is not a file: {path}")
    return str(path)


def _summarize_pipeline_result(result: dict[str, Any]) -> dict[str, Any]:
    summary = dict(result)
    affected_visit_ids = summary.pop("affected_visit_ids", None)
    if affected_visit_ids is not None:
        summary["affected_visit_count"] = len(affected_visit_ids)
    return summary


def _env_value(*names: str, default: str = "") -> str:
    import os

    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return default


def _build_options(
    *,
    source_mode: str,
    include_data_dump: bool,
    include_coverage: bool,
    data_dump_file: str | None = None,
    coverage_file: str | None = None,
    run_validation: bool = False,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> dict[str, Any]:
    mode = source_mode.lower().strip()
    if mode not in SOURCE_MODES:
        raise ValueError("source_mode must be 'local' or 'portal'.")

    return {
        "source_mode": mode,
        "include_data_dump": bool(include_data_dump),
        "include_coverage": bool(include_coverage),
        "data_dump_file": data_dump_file,
        "coverage_file": coverage_file,
        "run_validation": bool(run_validation),
        "start_date": _to_iso(start_date),
        "end_date": _to_iso(end_date),
    }


def _run_flow_with_user_logger(flow_function, options: dict[str, Any], logger: UserLogger | None):
    previous_logger = _set_active_user_logger(logger)
    try:
        return flow_function(options)
    finally:
        _set_active_user_logger(previous_logger)


@task(name="check configuration")
def check_configuration_task(options: dict[str, Any]) -> dict[str, Any]:
    load_project_env()

    mode = options["source_mode"]
    include_data_dump = bool(options["include_data_dump"])
    include_coverage = bool(options["include_coverage"])
    data_dump_file = options.get("data_dump_file")
    coverage_file = options.get("coverage_file")
    start_date = _date_from_iso(options.get("start_date"))
    end_date = _date_from_iso(options.get("end_date"))

    if start_date and end_date and start_date > end_date:
        raise ValueError("start-date cannot be after end-date.")
    if not include_data_dump and not include_coverage:
        raise ValueError("No dataset selected for the monitored run.")

    _emit("Step started: configuration check")
    _emit(f"Source mode: {mode}")
    _emit(f"Selected datasets: Data Dump={include_data_dump}, Coverage={include_coverage}")
    _emit(f"Validation after Data Dump: {bool(options['run_validation'])}")

    checked = dict(options)
    if mode == "local":
        if include_data_dump:
            if not data_dump_file:
                raise ValueError("Local Data Dump mode requires a Data Dump Excel file path.")
            checked["data_dump_file"] = _resolve_existing_file(data_dump_file, "Data Dump")
            _emit(f"Data Dump file: {checked['data_dump_file']}")

        if include_coverage:
            if not coverage_file:
                raise ValueError("Local Coverage mode requires a Coverage Excel file path.")
            checked["coverage_file"] = _resolve_existing_file(coverage_file, "Coverage")
            _emit(f"Coverage file: {checked['coverage_file']}")

    if mode == "portal":
        required_portal_values = {
            "PORTAL_USERNAME": ("PORTAL_USERNAME", "PORTAL_USER"),
            "PORTAL_PASSWORD": ("PORTAL_PASSWORD", "PORTAL_PASS"),
        }
        missing = [
            canonical_name
            for canonical_name, aliases in required_portal_values.items()
            if not _env_value(*aliases)
        ]
        if missing:
            raise RuntimeError(f"Portal mode is missing environment variables: {', '.join(missing)}")
        _emit("Portal credentials found in environment/.env; secret values are not logged.")

    if checked["run_validation"] and not include_data_dump:
        _emit("Validation was requested, but Data Dump is not selected; validation will not run.", "warning")

    _emit("Step succeeded: configuration check")
    return checked


@task(name="check database connection", retries=2, retry_delay_seconds=10)
def check_database_connection_task() -> dict[str, Any]:
    safe_config = {
        "host": DB_CONFIG.get("host"),
        "port": DB_CONFIG.get("port"),
        "database": DB_CONFIG.get("database"),
    }
    _emit(
        "Step started: database connection check "
        f"({safe_config['host']}:{safe_config['port']}/{safe_config['database']})"
    )

    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        _emit("Step succeeded: database connection check")
        return safe_config
    except Exception as exc:
        _emit(f"Step failed: database connection check - {exc}", "error")
        raise RuntimeError(f"Database connection check failed: {exc}") from exc
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


@task(name="download portal files", retries=2, retry_delay_seconds=30)
def download_portal_files_task(options: dict[str, Any]) -> dict[str, str]:
    include_data_dump = bool(options["include_data_dump"])
    include_coverage = bool(options["include_coverage"])
    start_date = _date_from_iso(options.get("start_date"))
    end_date = _date_from_iso(options.get("end_date"))

    _emit(
        "Step started: portal download "
        f"(Data Dump={include_data_dump}, Coverage={include_coverage})"
    )
    try:
        downloads = download_daily_reports_from_portal(
            include_data_dump=include_data_dump,
            include_coverage=include_coverage,
            coverage_start_date=start_date,
            coverage_end_date=end_date,
        )
    except Exception as exc:
        _emit(f"Step failed: portal download - {exc}", "error")
        raise

    for dataset, file_path in downloads.items():
        _emit(f"Portal downloaded {dataset}: {file_path}")
    _emit("Step succeeded: portal download")
    return downloads


@task(name="run Data Dump ETL")
def run_data_dump_pipeline_task(file_path: str, options: dict[str, Any]) -> dict[str, Any]:
    source_mode = options["source_mode"]
    run_validation = bool(options["run_validation"])
    start_date = _date_from_iso(options.get("start_date"))
    end_date = _date_from_iso(options.get("end_date"))

    _emit(f"Step started: Data Dump ETL ({file_path})")
    _emit(f"Validation status: {'enabled' if run_validation else 'skipped'}")
    try:
        result = run_data_dump_pipeline(
            file_path,
            downloaded_from_portal=(source_mode == "portal"),
            should_run_validation=run_validation,
            start_date=start_date,
            end_date=end_date,
            source_mode=source_mode,
            logger=_pipeline_logger,
        )
    except Exception as exc:
        _emit(f"Step failed: Data Dump ETL - {exc}", "error")
        raise

    summary = _summarize_pipeline_result(result)
    _emit(f"Step succeeded: Data Dump ETL - {summary}")
    return summary


@task(name="run Coverage ETL")
def run_coverage_pipeline_task(file_path: str, options: dict[str, Any]) -> dict[str, Any]:
    source_mode = options["source_mode"]

    _emit(f"Step started: Coverage ETL ({file_path})")
    try:
        result = run_coverage_pipeline(
            file_path,
            source_mode=source_mode,
            logger=_pipeline_logger,
        )
    except Exception as exc:
        _emit(f"Step failed: Coverage ETL - {exc}", "error")
        raise

    summary = _summarize_pipeline_result(result)
    _emit(f"Step succeeded: Coverage ETL - {summary}")
    return summary


@task(name="final summary")
def final_summary_task(
    options: dict[str, Any],
    database: dict[str, Any] | None,
    downloads: dict[str, str],
    data_dump_result: dict[str, Any] | None,
    coverage_result: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = {
        "source_mode": options["source_mode"],
        "database": database,
        "downloads": downloads,
        "data_dump_result": data_dump_result,
        "coverage_result": coverage_result,
        "validation_requested": bool(options["run_validation"]),
    }
    _emit(f"Monitored run summary: {summary}")
    return summary


@flow(name="Merch Performance Monitored Pipeline")
def monitored_pipeline_flow(options: dict[str, Any]) -> dict[str, Any]:
    checked_options = check_configuration_task(options)
    database = check_database_connection_task()

    downloads: dict[str, str] = {}
    if checked_options["source_mode"] == "portal":
        downloads = download_portal_files_task(checked_options)

    data_dump_result = None
    if checked_options["include_data_dump"]:
        data_dump_file = (
            downloads.get("data_dump")
            if checked_options["source_mode"] == "portal"
            else checked_options.get("data_dump_file")
        )
        if not data_dump_file:
            raise RuntimeError("Data Dump was selected but no file path was provided or downloaded.")
        data_dump_result = run_data_dump_pipeline_task(data_dump_file, checked_options)

    coverage_result = None
    if checked_options["include_coverage"]:
        coverage_file = (
            downloads.get("coverage")
            if checked_options["source_mode"] == "portal"
            else checked_options.get("coverage_file")
        )
        if not coverage_file:
            raise RuntimeError("Coverage was selected but no file path was provided or downloaded.")
        coverage_result = run_coverage_pipeline_task(coverage_file, checked_options)

    return final_summary_task(
        checked_options,
        database,
        downloads,
        data_dump_result,
        coverage_result,
    )


@flow(name="Merch Performance Portal Download")
def portal_download_flow(options: dict[str, Any]) -> dict[str, str]:
    checked_options = check_configuration_task(options)
    return download_portal_files_task(checked_options)


def run_data_dump_with_monitoring(
    file_path: str,
    run_validation: bool = False,
    *,
    source_mode: str = "local",
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    logger: UserLogger | None = print,
) -> dict[str, Any]:
    options = _build_options(
        source_mode=source_mode,
        include_data_dump=True,
        include_coverage=False,
        data_dump_file=file_path,
        run_validation=run_validation,
        start_date=start_date,
        end_date=end_date,
    )
    return _run_flow_with_user_logger(monitored_pipeline_flow, options, logger)


def run_coverage_with_monitoring(
    file_path: str,
    *,
    source_mode: str = "local",
    logger: UserLogger | None = print,
) -> dict[str, Any]:
    options = _build_options(
        source_mode=source_mode,
        include_data_dump=False,
        include_coverage=True,
        coverage_file=file_path,
    )
    return _run_flow_with_user_logger(monitored_pipeline_flow, options, logger)


def run_portal_download_with_monitoring(
    *,
    include_data_dump: bool = True,
    include_coverage: bool = True,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    logger: UserLogger | None = print,
) -> dict[str, str]:
    options = _build_options(
        source_mode="portal",
        include_data_dump=include_data_dump,
        include_coverage=include_coverage,
        start_date=start_date,
        end_date=end_date,
    )
    return _run_flow_with_user_logger(portal_download_flow, options, logger)


def run_full_pipeline_with_monitoring(
    *,
    include_data_dump: bool,
    include_coverage: bool,
    source_mode: str,
    data_dump_file: str | None = None,
    coverage_file: str | None = None,
    run_validation: bool = False,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    logger: UserLogger | None = print,
) -> dict[str, Any]:
    options = _build_options(
        source_mode=source_mode,
        include_data_dump=include_data_dump,
        include_coverage=include_coverage,
        data_dump_file=data_dump_file,
        coverage_file=coverage_file,
        run_validation=run_validation,
        start_date=start_date,
        end_date=end_date,
    )
    return _run_flow_with_user_logger(monitored_pipeline_flow, options, logger)
