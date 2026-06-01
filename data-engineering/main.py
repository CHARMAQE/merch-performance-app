import argparse
import sys
from datetime import datetime
from pathlib import Path

from launcher import launch_desktop_app
from orchestration.prefect_orchestrator import (
    run_coverage_with_monitoring,
    run_data_dump_with_monitoring,
    run_full_pipeline_with_monitoring,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load merch performance Excel data into MySQL."
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument("--file", dest="excel_file", help="Path to a local Data Dump Excel file.")
    source_group.add_argument("--portal", action="store_true", help="Download Data Dump from the portal.")
    source_group.add_argument("--coverage-file", dest="coverage_file", help="Path to a local Coverage Excel file.")
    source_group.add_argument("--coverage-portal", action="store_true", help="Download Coverage from the portal.")

    parser.add_argument("--start-date", help="Optional first visit date, formatted as YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Optional last visit date, formatted as YYYY-MM-DD.")

    validation_group = parser.add_mutually_exclusive_group()
    validation_group.add_argument("--run-validation", action="store_true", help="Run validation after Data Dump.")
    validation_group.add_argument("--skip-validation", action="store_true", help="Skip validation after Data Dump.")

    return parser.parse_args()


def parse_date_argument(value, argument_name):
    if not value:
        return None

    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{argument_name} must use YYYY-MM-DD format.") from exc


def require_existing_file(file_path):
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return str(path)


def resolve_validation_mode(args, downloaded_from_portal):
    if args.run_validation:
        return True
    if args.skip_validation:
        return False
    return downloaded_from_portal


def main():
    if len(sys.argv) == 1:
        launch_desktop_app()
        return

    args = parse_args()
    start_date = parse_date_argument(args.start_date, "--start-date")
    end_date = parse_date_argument(args.end_date, "--end-date")

    print("\n===== START PROJECT RUN =====")

    if args.coverage_file:
        excel_file = require_existing_file(args.coverage_file)
        run_coverage_with_monitoring(excel_file, source_mode="local", logger=print)
    elif args.coverage_portal:
        run_full_pipeline_with_monitoring(
            include_data_dump=False,
            include_coverage=True,
            source_mode="portal",
            start_date=start_date,
            end_date=end_date,
            logger=print,
        )
    else:
        if args.portal:
            run_full_pipeline_with_monitoring(
                include_data_dump=True,
                include_coverage=False,
                source_mode="portal",
                run_validation=resolve_validation_mode(args, downloaded_from_portal=True),
                start_date=start_date,
                end_date=end_date,
                logger=print,
            )
        else:
            excel_file = require_existing_file(args.excel_file)
            run_data_dump_with_monitoring(
                excel_file,
                run_validation=resolve_validation_mode(args, downloaded_from_portal=False),
                source_mode="local",
                start_date=start_date,
                end_date=end_date,
                logger=print,
            )

    print("\n===== PROJECT RUN COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    main()
