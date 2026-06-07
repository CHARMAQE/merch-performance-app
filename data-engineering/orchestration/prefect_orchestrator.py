from __future__ import annotations

import os
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import mysql.connector
import pandas as pd
from prefect import flow, get_run_logger, task

from config.db_config import DB_CONFIG
from config.env_loader import load_project_env
from extract.portal_exporter import download_daily_reports_from_portal
from load.etl_run_log import finish_etl_run, start_etl_run
from load.idempotency import cleanup_existing_data_for_visits
from load.load_base_tables import load_employees, load_products, load_stores, load_visits
from load.load_coverage import load_coverage
from load.load_survey_responses import fetch_visit_lookup_dataframe, load_survey_responses
from load.load_task_tables import load_task_tables
from pipelines import filter_source_dataframe_by_date
from transform.build_base_tables import (
    build_employees_dataframe,
    build_products_dataframe,
    build_stores_dataframe,
    build_visits_dataframe,
    normalize_source_dataframe,
    read_source_excel,
)
from transform.build_coverage import build_coverage_dataframe, read_coverage_excel
from transform.build_survey_responses import build_survey_responses_dataframe
from transform.build_task_tables import build_tagged_task_dataframe, build_task_table_batches
from validation.engine.registry import REGISTERED_RULES
from validation.engine.validation_engine import _load_active_rule_codes, _sync_registered_rules
from validation.rules import gps_inconsistent_checkin_same_store_month, osa_unusual_non
from validation.validation_runner import finish_validation_run, insert_validation_run


SOURCE_MODES = {"local", "portal"}
DATA_ENGINEERING_DIR = Path(__file__).resolve().parents[1]
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


def _file_metadata(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "file_name": path.name,
        "size_bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
    }


def _summarize_pipeline_result(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if result is None:
        return None

    summary = dict(result)
    affected_visit_ids = summary.pop("affected_visit_ids", None)
    if affected_visit_ids is not None:
        summary["affected_visit_count"] = len(affected_visit_ids)
    return summary


def _env_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return default


def _date_bounds_from_dataframe(df: pd.DataFrame, column_name: str) -> tuple[Any | None, Any | None]:
    if df.empty or column_name not in df:
        return None, None

    values = pd.to_datetime(df[column_name], errors="coerce")
    values = values[values.notna()]
    if values.empty:
        return None, None

    return values.min().date(), values.max().date()


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


def _open_db_cursor(dictionary: bool = False):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=dictionary)
    return conn, cursor


def _validate_options(options: dict[str, Any]) -> dict[str, Any]:
    load_project_env()

    mode = options["source_mode"]
    include_data_dump = bool(options["include_data_dump"])
    include_coverage = bool(options["include_coverage"])
    start_date = _date_from_iso(options.get("start_date"))
    end_date = _date_from_iso(options.get("end_date"))

    if start_date and end_date and start_date > end_date:
        raise ValueError("start-date cannot be after end-date.")
    if not include_data_dump and not include_coverage:
        raise ValueError("No dataset selected for the monitored run.")

    _emit("Step started: environment/options check")
    _emit(f"Source mode: {mode}")
    _emit(f"Selected datasets: Data Dump={include_data_dump}, Coverage={include_coverage}")
    _emit(f"Validation after Data Dump: {bool(options['run_validation'])}")
    if start_date or end_date:
        _emit(f"Visit date filter: {start_date or 'first available'} to {end_date or 'last available'}")

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

    if options["run_validation"] and not include_data_dump:
        _emit("Validation was requested, but Data Dump is not selected; validation will not run.", "warning")

    _emit("Step succeeded: environment/options check")
    return dict(options)


def _check_mysql_connection() -> dict[str, Any]:
    safe_config = {
        "host": DB_CONFIG.get("host"),
        "port": DB_CONFIG.get("port"),
        "database": DB_CONFIG.get("database"),
    }
    _emit(
        "Step started: MySQL connection check "
        f"({safe_config['host']}:{safe_config['port']}/{safe_config['database']})"
    )

    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        _emit("Step succeeded: MySQL connection check")
        return safe_config
    except Exception as exc:
        _emit(f"Step failed: MySQL connection check - {exc}", "error")
        raise RuntimeError(f"Database connection check failed: {exc}") from exc
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _download_or_resolve_source_files(options: dict[str, Any]) -> dict[str, Any]:
    source_mode = options["source_mode"]
    downloads: dict[str, str] = {}
    files: dict[str, str] = {}

    _emit("Step started: portal download or local source resolution")

    if source_mode == "portal":
        include_data_dump = bool(options["include_data_dump"])
        include_coverage = bool(options["include_coverage"])
        start_date = _date_from_iso(options.get("start_date"))
        end_date = _date_from_iso(options.get("end_date"))

        _emit(
            "Downloading selected portal exports "
            f"(Data Dump={include_data_dump}, Coverage={include_coverage})"
        )
        downloads = download_daily_reports_from_portal(
            include_data_dump=include_data_dump,
            include_coverage=include_coverage,
            coverage_start_date=start_date,
            coverage_end_date=end_date,
        )
        for dataset, file_path in downloads.items():
            _emit(f"Portal downloaded {dataset}: {file_path}")

    if options["include_data_dump"]:
        files["data_dump"] = (
            downloads.get("data_dump")
            if source_mode == "portal"
            else options.get("data_dump_file")
        )
        if not files["data_dump"]:
            raise RuntimeError("Data Dump was selected but no file path was provided or downloaded.")

    if options["include_coverage"]:
        files["coverage"] = (
            downloads.get("coverage")
            if source_mode == "portal"
            else options.get("coverage_file")
        )
        if not files["coverage"]:
            raise RuntimeError("Coverage was selected but no file path was provided or downloaded.")

    for dataset, file_path in files.items():
        _emit(f"Resolved {dataset} input: {file_path}")

    _emit("Step succeeded: portal download or local source resolution")
    return {"files": files, "downloads": downloads}


def _validate_excel_files(input_files: dict[str, str]) -> dict[str, Any]:
    _emit("Step started: Excel validation")
    validated_files: dict[str, str] = {}
    metadata: dict[str, dict[str, Any]] = {}

    for dataset, file_path in input_files.items():
        label = "Data Dump" if dataset == "data_dump" else "Coverage"
        resolved = Path(_resolve_existing_file(file_path, label))
        if resolved.suffix.lower() not in {".xls", ".xlsx", ".xlsm"}:
            _emit(f"{label} file extension is unusual for Excel: {resolved.suffix}", "warning")

        validated_files[dataset] = str(resolved)
        metadata[dataset] = _file_metadata(resolved)
        _emit(
            f"{label} file validated: {resolved.name} "
            f"({metadata[dataset]['size_bytes']} bytes)"
        )

    _emit("Step succeeded: Excel validation")
    return {"files": validated_files, "metadata": metadata}


def _backup_raw_files(input_files: dict[str, str]) -> dict[str, Any]:
    load_project_env()
    backup_root = Path(_env_value("BACKUP_DIR", default=str(DATA_ENGINEERING_DIR / "backups"))).expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    traceability: dict[str, dict[str, Any]] = {}

    _emit(f"Step started: raw backup / traceability ({backup_root})")
    for dataset, file_path in input_files.items():
        source = Path(file_path).expanduser().resolve()
        dataset_dir = backup_root / dataset / datetime.now().strftime("%Y-%m")
        target = dataset_dir / f"{timestamp}_{source.name}"
        record = _file_metadata(source)
        record["backup_path"] = None
        record["backup_status"] = "not_attempted"

        try:
            dataset_dir.mkdir(parents=True, exist_ok=True)
            if source != target:
                shutil.copy2(source, target)
            record["backup_path"] = str(target)
            record["backup_status"] = "copied"
            _emit(f"Raw {dataset} file copied for traceability: {target}")
        except Exception as exc:
            record["backup_status"] = "warning"
            record["backup_warning"] = str(exc)
            _emit(
                f"Raw {dataset} backup could not be created; continuing with original file. Reason: {exc}",
                "warning",
            )

        traceability[dataset] = record

    _emit("Step succeeded: raw backup / traceability")
    return traceability


def _initialize_data_dump_run(file_path: str, options: dict[str, Any]) -> int:
    run_id = start_etl_run(
        "DATA_DUMP",
        options["source_mode"],
        file_path,
        validation_enabled=bool(options["run_validation"]),
    )
    _emit(f"Data Dump ETL run log initialized: run_id={run_id}")
    return int(run_id)


def _finish_data_dump_run(
    run_id: int,
    status: str,
    source_summary: dict[str, Any] | None = None,
    etl_result: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    source_summary = source_summary or {}
    etl_result = etl_result or {}

    finish_etl_run(
        run_id,
        status,
        date_from=source_summary.get("date_from"),
        date_to=source_summary.get("date_to"),
        rows_read=source_summary.get("rows_read"),
        rows_loaded=etl_result.get("rows"),
        rows_inserted=etl_result.get("survey_responses", 0) if status == "SUCCESS" else None,
        rows_updated=len(etl_result.get("affected_visit_ids", [])) if status == "SUCCESS" else None,
        affected_visits=len(etl_result.get("affected_visit_ids", [])) if status == "SUCCESS" else None,
        survey_responses=etl_result.get("survey_responses", 0) if status == "SUCCESS" else None,
        error_message=error_message,
    )
    _emit(f"Data Dump ETL run log finished with status={status}")
    return {"run_id": run_id, "status": status, "error_message": error_message}


def _read_and_normalize_data_dump(file_path: str, options: dict[str, Any]) -> dict[str, Any]:
    _emit(f"Step started: read and normalize Data Dump ({file_path})")
    raw_df = read_source_excel(file_path)
    _emit(f"Data Dump rows read: {len(raw_df)}")
    _emit(f"Data Dump columns detected: {len(raw_df.columns)}")

    normalized_df = normalize_source_dataframe(raw_df)
    invalid_dates = int(normalized_df["date"].isna().sum()) if "date" in normalized_df else 0
    if invalid_dates:
        _emit(f"Data Dump rows with invalid visit date: {invalid_dates}", "warning")

    source_df = filter_source_dataframe_by_date(
        normalized_df,
        _date_from_iso(options.get("start_date")),
        _date_from_iso(options.get("end_date")),
    )
    date_from, date_to = _date_bounds_from_dataframe(source_df, "date")
    source_summary = {
        "rows_read": len(source_df),
        "date_from": date_from,
        "date_to": date_to,
    }

    _emit(f"Data Dump rows retained for load: {len(source_df)}")
    _emit(f"Data Dump load window: {date_from} to {date_to}")
    _emit("Step succeeded: read and normalize Data Dump")
    return {"source_df": source_df, "source_summary": source_summary}


def _prepare_load_dimensions_and_visits(source_df: pd.DataFrame) -> dict[str, Any]:
    _emit("Step started: prepare/load dimensions and visits")

    employees_df = build_employees_dataframe(source_df)
    stores_df = build_stores_dataframe(source_df)
    products_df = build_products_dataframe(source_df)
    visits_df = build_visits_dataframe(source_df)
    _emit(
        "Prepared dataframe counts: "
        f"employees={len(employees_df)}, stores={len(stores_df)}, "
        f"products={len(products_df)}, visits={len(visits_df)}"
    )

    conn, cursor = _open_db_cursor()
    try:
        employee_map = load_employees(conn, cursor, employees_df, logger=_pipeline_logger)
        store_map = load_stores(conn, cursor, stores_df, logger=_pipeline_logger)
        product_map = load_products(conn, cursor, products_df, logger=_pipeline_logger)
        visit_map = load_visits(
            conn,
            cursor,
            visits_df,
            employee_map,
            store_map,
            logger=_pipeline_logger,
        )
        affected_visit_ids = sorted({int(v) for v in visit_map.values() if v is not None})
        _emit(f"Affected visit_ids in uploaded file: {len(affected_visit_ids)}")

        cleanup_result = cleanup_existing_data_for_visits(
            conn,
            cursor,
            affected_visit_ids,
            logger=_pipeline_logger,
        )
        _emit(
            "Idempotency cleanup completed: "
            f"task_rows_deleted={cleanup_result.get('task_rows_deleted', 0)}, "
            f"survey_responses_deleted={cleanup_result.get('survey_responses_deleted', 0)}"
        )

        _emit("Step succeeded: prepare/load dimensions and visits")
        return {
            "employee_map": employee_map,
            "store_map": store_map,
            "product_map": product_map,
            "visit_map": visit_map,
            "affected_visit_ids": affected_visit_ids,
            "cleanup_result": cleanup_result,
            "employees": len(employee_map),
            "stores": len(store_map),
            "products": len(product_map),
            "visits": len(visit_map),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def _generate_load_task_tables(
    source_df: pd.DataFrame,
    visit_map: dict,
    product_map: dict,
    affected_visit_ids: list[int],
) -> dict[str, Any]:
    _emit("Step started: generate/load task tables")
    tagged_df = build_tagged_task_dataframe(source_df, visit_map, product_map)
    task_batches = build_task_table_batches(tagged_df)
    task_tables = [batch["table_name"] for batch in task_batches]
    _emit(
        f"Task rows matched: {len(tagged_df)}; task table batches={len(task_batches)}; "
        f"tables={task_tables}"
    )

    conn, cursor = _open_db_cursor()
    try:
        load_task_tables(
            conn,
            cursor,
            task_batches,
            affected_visit_ids,
            logger=_pipeline_logger,
            cleanup_existing=False,
        )
        _emit("Step succeeded: generate/load task tables")
        return {
            "task_table_count": len(task_batches),
            "task_tables": task_tables,
            "tagged_row_count": len(tagged_df),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def _build_load_survey_responses(source_df: pd.DataFrame) -> dict[str, Any]:
    _emit("Step started: build/load survey_responses")
    visit_lookup_df = fetch_visit_lookup_dataframe()
    _emit(f"Visits lookup rows fetched: {len(visit_lookup_df)}")

    survey_df = build_survey_responses_dataframe(source_df, visit_lookup_df)
    _emit(f"survey_responses rows ready for load: {len(survey_df)}")

    if survey_df.empty:
        _emit("No valid rows after survey_responses transformation; validation will be skipped.")
        return {
            "survey_responses": 0,
            "validation_eligible": False,
        }

    inserted_rows = load_survey_responses(
        survey_df,
        cleanup_existing=False,
        logger=_pipeline_logger,
    )
    _emit(f"survey_responses loading completed: inserted_rows={inserted_rows}")
    _emit("Step succeeded: build/load survey_responses")
    return {
        "survey_responses": int(inserted_rows),
        "validation_eligible": True,
    }


def _initialize_coverage_run(file_path: str, options: dict[str, Any]) -> int:
    run_id = start_etl_run("COVERAGE", options["source_mode"], file_path)
    _emit(f"Coverage ETL run log initialized: run_id={run_id}")
    return int(run_id)


def _finish_coverage_run(
    run_id: int,
    status: str,
    source_summary: dict[str, Any] | None = None,
    coverage_result: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    source_summary = source_summary or {}
    coverage_result = coverage_result or {}

    finish_etl_run(
        run_id,
        status,
        date_from=source_summary.get("date_from"),
        date_to=source_summary.get("date_to"),
        rows_read=source_summary.get("rows_read"),
        rows_loaded=coverage_result.get("rows"),
        rows_inserted=coverage_result.get("inserted") if status == "SUCCESS" else None,
        rows_updated=coverage_result.get("updated") if status == "SUCCESS" else None,
        error_message=error_message,
    )
    _emit(f"Coverage ETL run log finished with status={status}")
    return {"run_id": run_id, "status": status, "error_message": error_message}


def _read_and_normalize_coverage(file_path: str) -> dict[str, Any]:
    _emit(f"Step started: read and normalize Coverage ({file_path})")
    raw_df = read_coverage_excel(file_path)
    _emit(f"Coverage rows read: {len(raw_df)}")
    _emit(f"Coverage columns detected: {len(raw_df.columns)}")

    coverage_df = build_coverage_dataframe(raw_df)
    date_from, date_to = _date_bounds_from_dataframe(coverage_df, "visit_date")
    source_summary = {
        "rows_read": len(coverage_df),
        "date_from": date_from,
        "date_to": date_to,
    }
    _emit(f"Coverage rows after normalization/deduplication: {len(coverage_df)}")
    _emit(f"fact_coverage load window: {date_from} to {date_to}")
    _emit("Step succeeded: read and normalize Coverage")
    return {"coverage_df": coverage_df, "source_summary": source_summary}


def _prepare_load_fact_coverage(coverage_df: pd.DataFrame) -> dict[str, Any]:
    _emit("Step started: prepare/load fact_coverage")
    if coverage_df.empty:
        _emit("Process stopped: no valid Coverage rows found.")
        return {"rows": 0, "inserted": 0, "updated": 0}

    result = load_coverage(coverage_df)
    _emit(
        "fact_coverage load completed: "
        f"{result['inserted']} inserted, {result['updated']} updated, "
        f"{result['rows']} total processed."
    )
    _emit("Step succeeded: prepare/load fact_coverage")
    return result


def _finish_validation_run_as_failed(validation_run: dict[str, Any], error_message: str) -> dict[str, Any]:
    run_id = int(validation_run["run_id"])
    conn, cursor = _open_db_cursor()
    try:
        finish_validation_run(
            cursor,
            run_id=run_id,
            status="FAILED",
            rules_executed=0,
            issues_found=0,
            error_message=error_message[:4000],
        )
        conn.commit()
        _emit(f"Validation run marked FAILED: run_id={run_id}", "error")
        return {"run_id": run_id, "status": "FAILED", "error_message": error_message}
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def _execute_validation_rule(rule, label: str, validation_run: dict[str, Any], active_rule_codes: list[str]) -> dict[str, Any]:
    rule_code = rule.RULE_CODE
    if rule_code not in set(active_rule_codes):
        _emit(f"{label} skipped because rule is inactive: {rule_code}")
        return {"rule_code": rule_code, "status": "SKIPPED", "issues_found": 0}

    _emit(f"Step started: {label} ({rule_code})")
    issues_found = int(
        rule.run(
            int(validation_run["run_id"]),
            target_visit_ids=validation_run["target_visit_ids"],
        )
    )
    _emit(f"{label} completed: issues_found={issues_found}")
    return {"rule_code": rule_code, "status": "SUCCESS", "issues_found": issues_found}


@task(name="Pre-run - environment/options check")
def check_environment_options_task(options: dict[str, Any]) -> dict[str, Any]:
    return _validate_options(options)


@task(name="Pre-run - MySQL connection check", retries=2, retry_delay_seconds=10)
def check_mysql_connection_task() -> dict[str, Any]:
    return _check_mysql_connection()


@flow(name="Pre-run checks")
def pre_run_checks_flow(options: dict[str, Any]) -> dict[str, Any]:
    checked_options = check_environment_options_task(options)
    database = check_mysql_connection_task()
    return {"options": checked_options, "database": database}


@task(name="Input - acquire source files", retries=2, retry_delay_seconds=30)
def acquire_source_files_task(options: dict[str, Any]) -> dict[str, Any]:
    return _download_or_resolve_source_files(options)


@task(name="Input - validate Excel files")
def validate_excel_files_task(input_files: dict[str, str]) -> dict[str, Any]:
    return _validate_excel_files(input_files)


@task(name="Input - backup raw files")
def backup_raw_files_task(input_files: dict[str, str]) -> dict[str, Any]:
    return _backup_raw_files(input_files)


@flow(name="Input acquisition and traceability")
def input_acquisition_flow(options: dict[str, Any]) -> dict[str, Any]:
    acquired = acquire_source_files_task(options)
    validated = validate_excel_files_task(acquired["files"])
    traceability = backup_raw_files_task(validated["files"])
    return {
        "files": validated["files"],
        "metadata": validated["metadata"],
        "downloads": acquired["downloads"],
        "traceability": traceability,
    }


@task(name="Data Dump - read and normalize")
def read_normalize_data_dump_task(file_path: str, options: dict[str, Any]) -> dict[str, Any]:
    return _read_and_normalize_data_dump(file_path, options)


@task(name="Data Dump - prepare/load dimensions and visits")
def prepare_load_dimensions_visits_task(source_df: pd.DataFrame) -> dict[str, Any]:
    return _prepare_load_dimensions_and_visits(source_df)


@task(name="Data Dump - generate/load task tables")
def generate_load_task_tables_task(
    source_df: pd.DataFrame,
    visit_map: dict,
    product_map: dict,
    affected_visit_ids: list[int],
) -> dict[str, Any]:
    return _generate_load_task_tables(source_df, visit_map, product_map, affected_visit_ids)


@task(name="Data Dump - build/load survey_responses")
def build_load_survey_responses_task(source_df: pd.DataFrame) -> dict[str, Any]:
    return _build_load_survey_responses(source_df)


@task(name="Data Dump - finish run log")
def finish_data_dump_run_log_task(
    run_id: int,
    status: str,
    source_summary: dict[str, Any] | None = None,
    etl_result: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    return _finish_data_dump_run(run_id, status, source_summary, etl_result, error_message)


@flow(name="Data Dump processing")
def data_dump_processing_flow(file_path: str, options: dict[str, Any]) -> dict[str, Any]:
    run_id = _initialize_data_dump_run(file_path, options)
    source_summary: dict[str, Any] | None = None
    etl_result: dict[str, Any] | None = None

    try:
        normalized = read_normalize_data_dump_task(file_path, options)
        source_df = normalized["source_df"]
        source_summary = normalized["source_summary"]

        if source_df.empty:
            _emit("Process stopped: no Data Dump rows matched the selected date filter.")
            etl_result = {
                "rows": 0,
                "affected_visit_ids": [],
                "survey_responses": 0,
                "validation_eligible": False,
            }
            finish_data_dump_run_log_task(run_id, "SUCCESS", source_summary, etl_result)
            return etl_result

        base_visit_load = prepare_load_dimensions_visits_task(source_df)
        task_load = generate_load_task_tables_task(
            source_df,
            base_visit_load["visit_map"],
            base_visit_load["product_map"],
            base_visit_load["affected_visit_ids"],
        )
        survey_load = build_load_survey_responses_task(source_df)

        etl_result = {
            "rows": len(source_df),
            "employees": base_visit_load["employees"],
            "stores": base_visit_load["stores"],
            "products": base_visit_load["products"],
            "visits": base_visit_load["visits"],
            "affected_visit_ids": base_visit_load["affected_visit_ids"],
            "idempotency_cleanup": base_visit_load["cleanup_result"],
            "task_tables": task_load,
            "survey_responses": survey_load["survey_responses"],
            "validation_eligible": survey_load["validation_eligible"],
        }
        finish_data_dump_run_log_task(run_id, "SUCCESS", source_summary, etl_result)
        _emit(f"Data Dump processing completed: {_summarize_pipeline_result(etl_result)}")
        return etl_result
    except Exception as exc:
        finish_data_dump_run_log_task(
            run_id,
            "FAILED",
            source_summary,
            etl_result,
            error_message=str(exc),
        )
        _emit(f"Data Dump processing failed: {exc}", "error")
        raise


@task(name="Validation - initialize validation run")
def initialize_validation_run_task(target_visit_ids: list[int]) -> dict[str, Any]:
    normalized_target_visit_ids = sorted({int(visit_id) for visit_id in target_visit_ids})
    conn, cursor = _open_db_cursor()
    try:
        run_id = insert_validation_run(cursor)
        conn.commit()
        _emit(f"Validation run initialized: run_id={run_id}")
        _emit(f"Validation scope: {len(normalized_target_visit_ids)} uploaded visits")
        return {"run_id": int(run_id), "target_visit_ids": normalized_target_visit_ids}
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


@task(name="Validation - sync active rules")
def sync_active_validation_rules_task() -> dict[str, Any]:
    _emit("Step started: sync active validation rules")
    conn, cursor = _open_db_cursor()
    try:
        _sync_registered_rules(cursor)
        conn.commit()
        active_rule_codes = sorted(_load_active_rule_codes(cursor))
        registered_rule_codes = [rule.RULE_CODE for rule in REGISTERED_RULES]
        _emit(
            "Validation rules synchronized: "
            f"registered={registered_rule_codes}, active={active_rule_codes}"
        )
        return {
            "registered_rule_codes": registered_rule_codes,
            "active_rule_codes": active_rule_codes,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


@task(name="Validation - execute OSA MT rule")
def execute_osa_mt_rule_task(validation_run: dict[str, Any], active_rule_codes: list[str]) -> dict[str, Any]:
    return _execute_validation_rule(
        osa_unusual_non,
        "OSA MT rule execution",
        validation_run,
        active_rule_codes,
    )


@task(name="Validation - execute GPS GT rule")
def execute_gps_gt_rule_task(validation_run: dict[str, Any], active_rule_codes: list[str]) -> dict[str, Any]:
    return _execute_validation_rule(
        gps_inconsistent_checkin_same_store_month,
        "GPS GT rule execution",
        validation_run,
        active_rule_codes,
    )


@task(name="Validation - load validation summary")
def load_validation_summary_task(validation_run: dict[str, Any], rule_results: list[dict[str, Any]]) -> dict[str, Any]:
    run_id = int(validation_run["run_id"])
    executed_results = [result for result in rule_results if result["status"] == "SUCCESS"]
    issues_found = sum(int(result["issues_found"]) for result in executed_results)
    rules_executed = len(executed_results)

    conn, cursor = _open_db_cursor()
    try:
        finish_validation_run(
            cursor,
            run_id=run_id,
            status="SUCCESS",
            rules_executed=rules_executed,
            issues_found=issues_found,
        )
        conn.commit()
        _emit(
            "Validation summary loaded: "
            f"run_id={run_id}, rules_executed={rules_executed}, issues_found={issues_found}"
        )
        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "rules_executed": rules_executed,
            "issues_found": issues_found,
            "rule_results": rule_results,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


@task(name="Validation - mark run failed")
def fail_validation_run_task(validation_run: dict[str, Any], error_message: str) -> dict[str, Any]:
    return _finish_validation_run_as_failed(validation_run, error_message)


@flow(name="Validation processing")
def validation_processing_flow(affected_visit_ids: list[int]) -> dict[str, Any]:
    validation_run = initialize_validation_run_task(affected_visit_ids)
    try:
        rule_state = sync_active_validation_rules_task()
        osa_result = execute_osa_mt_rule_task(validation_run, rule_state["active_rule_codes"])
        gps_result = execute_gps_gt_rule_task(validation_run, rule_state["active_rule_codes"])
        return load_validation_summary_task(validation_run, [osa_result, gps_result])
    except Exception as exc:
        fail_validation_run_task(validation_run, str(exc))
        raise


@task(name="Coverage - read and normalize")
def read_normalize_coverage_task(file_path: str) -> dict[str, Any]:
    return _read_and_normalize_coverage(file_path)


@task(name="Coverage - prepare/load fact_coverage")
def prepare_load_fact_coverage_task(coverage_df: pd.DataFrame) -> dict[str, Any]:
    return _prepare_load_fact_coverage(coverage_df)


@task(name="Coverage - finish run log")
def finish_coverage_run_log_task(
    run_id: int,
    status: str,
    source_summary: dict[str, Any] | None = None,
    coverage_result: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    return _finish_coverage_run(run_id, status, source_summary, coverage_result, error_message)


@flow(name="Coverage processing")
def coverage_processing_flow(file_path: str, options: dict[str, Any]) -> dict[str, Any]:
    run_id = _initialize_coverage_run(file_path, options)
    source_summary: dict[str, Any] | None = None
    coverage_result: dict[str, Any] | None = None

    try:
        normalized = read_normalize_coverage_task(file_path)
        source_summary = normalized["source_summary"]
        coverage_result = prepare_load_fact_coverage_task(normalized["coverage_df"])
        finish_coverage_run_log_task(run_id, "SUCCESS", source_summary, coverage_result)
        return coverage_result
    except Exception as exc:
        finish_coverage_run_log_task(
            run_id,
            "FAILED",
            source_summary,
            coverage_result,
            error_message=str(exc),
        )
        _emit(f"Coverage processing failed: {exc}", "error")
        raise


@task(name="Final execution summary")
def final_summary_task(
    options: dict[str, Any],
    database: dict[str, Any] | None,
    downloads: dict[str, str],
    traceability: dict[str, Any],
    data_dump_result: dict[str, Any] | None,
    validation_result: dict[str, Any] | None,
    coverage_result: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = {
        "source_mode": options["source_mode"],
        "database": database,
        "downloads": downloads,
        "traceability": traceability,
        "data_dump_result": _summarize_pipeline_result(data_dump_result),
        "validation_result": validation_result,
        "coverage_result": coverage_result,
        "validation_requested": bool(options["run_validation"]),
    }
    _emit(f"Monitored run summary: {summary}")
    return summary


@flow(name="Merch Performance Daily ETL Orchestration")
def monitored_pipeline_flow(options: dict[str, Any]) -> dict[str, Any]:
    pre_run = pre_run_checks_flow(options)
    checked_options = pre_run["options"]
    input_bundle = input_acquisition_flow(checked_options)

    data_dump_result = None
    validation_result = None
    if checked_options["include_data_dump"]:
        data_dump_result = data_dump_processing_flow(input_bundle["files"]["data_dump"], checked_options)

        if checked_options["run_validation"] and data_dump_result.get("validation_eligible"):
            validation_result = validation_processing_flow(data_dump_result.get("affected_visit_ids", []))
        elif checked_options["run_validation"]:
            _emit("Validation requested but skipped because no survey_responses rows were generated.", "warning")

    coverage_result = None
    if checked_options["include_coverage"]:
        coverage_result = coverage_processing_flow(input_bundle["files"]["coverage"], checked_options)

    return final_summary_task(
        checked_options,
        pre_run["database"],
        input_bundle["downloads"],
        input_bundle["traceability"],
        data_dump_result,
        validation_result,
        coverage_result,
    )


@flow(name="Merch Performance Portal Download")
def portal_download_flow(options: dict[str, Any]) -> dict[str, str]:
    checked_options = check_environment_options_task(options)
    acquired = acquire_source_files_task(checked_options)
    return acquired["downloads"]


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
