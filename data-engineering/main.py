import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

from load.load_survey_responses import (
    fetch_visit_lookup_dataframe,
    load_survey_responses,
)
from transform.build_base_tables import prepare_source_dataframe
from transform.build_survey_responses import build_survey_responses_dataframe
from transform.etl_excel_to_mysql import run_etl
from validation.validation_runner import main as run_validation


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load merch performance Excel data into MySQL."
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--file",
        dest="excel_file",
        help="Path to a local Excel export file.",
    )
    source_group.add_argument(
        "--portal",
        action="store_true",
        help="Download the Excel export from the portal.",
    )

    parser.add_argument(
        "--start-date",
        help="Optional first visit date to load, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        help="Optional last visit date to load, formatted as YYYY-MM-DD.",
    )

    validation_group = parser.add_mutually_exclusive_group()
    validation_group.add_argument(
        "--run-validation",
        action="store_true",
        help="Run validation after loading the selected data.",
    )
    validation_group.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation after loading the selected data.",
    )

    return parser.parse_args()


def parse_date_argument(value: str | None, argument_name: str) -> date | None:
    if not value:
        return None

    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{argument_name} must use YYYY-MM-DD format.") from exc


def filter_source_dataframe_by_date(source_df, start_date: date | None, end_date: date | None):
    if start_date is None and end_date is None:
        return source_df

    filtered_df = source_df.copy()
    visit_dates = filtered_df["date"].dt.date

    if start_date is not None:
        filtered_df = filtered_df[visit_dates >= start_date]
        visit_dates = filtered_df["date"].dt.date

    if end_date is not None:
        filtered_df = filtered_df[visit_dates <= end_date]

    return filtered_df.reset_index(drop=True)


def choose_local_excel_file() -> str:
    excel_file = input("Enter Excel file path: ").strip()

    if not Path(excel_file).exists():
        raise FileNotFoundError(f"File not found: {excel_file}")

    return excel_file


def export_from_portal() -> str:
    from extract.portal_exporter import download_excel_from_portal

    print("\nDownloading Excel file from portal...")
    excel_file = download_excel_from_portal()
    print(f"Downloaded file: {excel_file}")
    return excel_file


def choose_input_source() -> tuple[str, bool]:
    while True:
        print("\nChoose data source:")
        print("1 - Excel file from computer")
        print("2 - Download from portal automatically")

        choice = input("Enter 1 or 2: ").strip()

        if choice == "1":
            return choose_local_excel_file(), False

        if choice == "2":
            return export_from_portal(), True

        print("Invalid choice. Please enter 1 or 2.")


def choose_load_mode() -> bool:
    while True:
        print("\nChoose load mode:")
        print("1 - History / backfill load (skip validation)")
        print("2 - Daily load (run validation)")

        choice = input("Enter 1 or 2: ").strip()

        if choice == "1":
            return False

        if choice == "2":
            return True

        print("Invalid choice. Please enter 1 or 2.")


def resolve_input_source(args) -> tuple[str, bool]:
    if args.excel_file:
        excel_file = Path(args.excel_file)
        if not excel_file.exists():
            raise FileNotFoundError(f"File not found: {excel_file}")
        return str(excel_file), False

    if args.portal:
        return export_from_portal(), True

    return choose_input_source()


def resolve_validation_mode(args, downloaded_from_portal: bool) -> bool:
    if args.run_validation:
        return True

    if args.skip_validation:
        return False

    if downloaded_from_portal:
        print("\nPortal download detected: using daily load mode with validation.")
        return True

    return choose_load_mode()


def resolve_date_filter(
        downloaded_from_portal: bool,
        start_date: date | None,
        end_date: date | None
) -> tuple[date | None, date | None]:
    if start_date is not None or end_date is not None:
        return start_date, end_date

    if downloaded_from_portal:
        yesterday = date.today() - timedelta(days=1)
        print(f"\nPortal export detected: filtering automatically for yesterday ({yesterday}).")
        return yesterday, yesterday

    return None, None


def main():
    args = parse_args()
    start_date = parse_date_argument(args.start_date, "--start-date")
    end_date = parse_date_argument(args.end_date, "--end-date")

    print("\n===== START PROJECT RUN =====")

    excel_file, downloaded_from_portal = resolve_input_source(args)
    should_run_validation = resolve_validation_mode(args, downloaded_from_portal)
    start_date, end_date = resolve_date_filter(downloaded_from_portal, start_date, end_date)

    if start_date and end_date and start_date > end_date:
        raise ValueError("--start-date cannot be after --end-date.")

    print(f"\nSource file: {excel_file}")

    print("\nReading Excel file once...")
    source_df = prepare_source_dataframe(excel_file)
    print(f"Source rows loaded in memory: {len(source_df)}")

    source_df = filter_source_dataframe_by_date(source_df, start_date, end_date)
    if start_date or end_date:
        print(
            "Rows after date filter "
            f"{start_date or 'first available'} to {end_date or 'last available'}: "
            f"{len(source_df)}"
        )

    if source_df.empty:
        print("\nProcess stopped: no rows matched the selected date filter.")
        return

    print("\nRunning core ETL...")
    etl_result = run_etl(source_df, logger=print)
    affected_visit_ids = etl_result.get("affected_visit_ids", [])
    print("\nCore ETL finished.")
    # print(etl_result)
    print(f"Affected visits in uploaded file: {len(affected_visit_ids)}")

    print("\nFetching visits lookup for survey_responses...")
    visit_lookup_df = fetch_visit_lookup_dataframe()

    print("\nBuilding survey_responses dataframe...")
    df = build_survey_responses_dataframe(source_df, visit_lookup_df)
    print(f"Transformed rows ready for survey_responses load: {len(df)}")

    if df.empty:
        print("\nProcess stopped: no valid rows after transformation.")
        return

    print("\nLoading survey_responses...")
    inserted_rows = load_survey_responses(df)
    print(f"Inserted rows into survey_responses: {inserted_rows}")

    if should_run_validation:
        print("\nRunning database validations...")
        run_validation(target_visit_ids=affected_visit_ids)
    else:
        print("\nValidation skipped for history/backfill load.")

    print("\n===== PROJECT RUN COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    main()
