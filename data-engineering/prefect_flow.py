from __future__ import annotations

import argparse
import os
from datetime import datetime

from config.env_loader import load_project_env
from orchestration.prefect_orchestrator import run_full_pipeline_with_monitoring


def _env_value(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return default


def _env_bool(name: str, default: bool = False) -> bool:
    value = _env_value(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _parse_date(value: str | None, argument_name: str):
    if not value:
        return None

    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{argument_name} must use YYYY-MM-DD format.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Developer/debug entry point for the Prefect-monitored ETL."
    )
    parser.add_argument("--mode", choices=["local", "portal"], default=_env_value("PREFECT_FLOW_MODE", default="local"))
    parser.add_argument("--portal", action="store_true", help="Shortcut for --mode portal.")
    parser.add_argument("--data-dump-file", default=_env_value("PREFECT_DATA_DUMP_FILE"))
    parser.add_argument("--coverage-file", default=_env_value("PREFECT_COVERAGE_FILE"))
    parser.add_argument("--run-data-dump", action="store_true", default=_env_bool("PREFECT_RUN_DATA_DUMP"))
    parser.add_argument("--run-coverage", action="store_true", default=_env_bool("PREFECT_RUN_COVERAGE"))
    parser.add_argument("--run-validation", action="store_true", default=_env_bool("PREFECT_RUN_VALIDATION"))
    parser.add_argument("--start-date", default=_env_value("PREFECT_START_DATE"))
    parser.add_argument("--end-date", default=_env_value("PREFECT_END_DATE"))
    return parser.parse_args()


def main() -> None:
    load_project_env()
    args = parse_args()
    source_mode = "portal" if args.portal else args.mode
    include_data_dump = bool(args.run_data_dump or args.data_dump_file)
    include_coverage = bool(args.run_coverage or args.coverage_file)

    if source_mode == "portal" and not include_data_dump and not include_coverage:
        include_data_dump = True

    run_full_pipeline_with_monitoring(
        include_data_dump=include_data_dump,
        include_coverage=include_coverage,
        source_mode=source_mode,
        data_dump_file=args.data_dump_file,
        coverage_file=args.coverage_file,
        run_validation=args.run_validation,
        start_date=_parse_date(args.start_date, "--start-date"),
        end_date=_parse_date(args.end_date, "--end-date"),
        logger=print,
    )


if __name__ == "__main__":
    main()
