from pathlib import Path

from extract.portal_exporter import download_excel_from_portal
from load.load_survey_responses import (
    fetch_visit_lookup_dataframe,
    load_survey_responses,
)
from transform.build_base_tables import prepare_source_dataframe
from transform.build_survey_responses import build_survey_responses_dataframe
from transform.etl_excel_to_mysql import run_etl
from validation.validation_runner import main as run_validation


def choose_local_excel_file() -> str:
    excel_file = input("Enter Excel file path: ").strip()

    if not Path(excel_file).exists():
        raise FileNotFoundError(f"File not found: {excel_file}")

    return excel_file


def export_from_portal() -> str:
    print("\nDownloading Excel file from portal...")
    excel_file = download_excel_from_portal()
    print(f"Downloaded file: {excel_file}")
    return excel_file


def choose_input_source() -> str:
    print("\nChoose data source:")
    print("1 - Excel file from computer")
    print("2 - Download from portal automatically")

    choice = input("Enter 1 or 2: ").strip()

    if choice == "1":
        return choose_local_excel_file()

    if choice == "2":
        return export_from_portal()

    raise ValueError("Invalid choice. Please enter 1 or 2.")


def main():
    print("\n===== START PROJECT RUN =====")

    excel_file = choose_input_source()
    print(f"\nSource file: {excel_file}")

    print("\nReading Excel file once...")
    source_df = prepare_source_dataframe(excel_file)
    print(f"Source rows loaded in memory: {len(source_df)}")

    print("\nRunning core ETL...")
    etl_result = run_etl(source_df, full_refresh=False, logger=print)
    print("\nCore ETL finished.")
    print(etl_result)

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

    print("\nRunning database validations...")
    run_validation()

    print("\n===== PROJECT RUN COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    main()
