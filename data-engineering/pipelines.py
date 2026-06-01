from datetime import date

from load.etl_run_log import finish_etl_run, start_etl_run
from load.load_coverage import load_coverage
from load.load_survey_responses import (
    fetch_visit_lookup_dataframe,
    load_survey_responses,
)
from transform.build_base_tables import prepare_source_dataframe
from transform.build_coverage import prepare_coverage_dataframe
from transform.build_survey_responses import build_survey_responses_dataframe
from transform.etl_excel_to_mysql import run_etl
from validation.validation_runner import main as run_validation


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


def run_coverage_pipeline(excel_file: str, source_mode: str = "local", logger=print) -> dict:
    run_id = start_etl_run("COVERAGE", source_mode, excel_file)
    date_from = None
    date_to = None
    rows_read = None

    logger(f"\nCoverage source file: {excel_file}")
    try:
        logger("\nReading and transforming Coverage file...")
        coverage_df = prepare_coverage_dataframe(excel_file)
        rows_read = len(coverage_df)
        logger(f"Coverage rows ready for load: {rows_read}")

        if not coverage_df.empty:
            date_from = coverage_df["visit_date"].min()
            date_to = coverage_df["visit_date"].max()

        if coverage_df.empty:
            logger("\nProcess stopped: no valid coverage rows found.")
            finish_etl_run(run_id, "SUCCESS", rows_read=0, rows_loaded=0, rows_inserted=0, rows_updated=0)
            return {"rows": 0, "inserted": 0, "updated": 0}

        logger("\nLoading fact_coverage...")
        result = load_coverage(coverage_df)
        logger(
            "fact_coverage load completed: "
            f"{result['inserted']} inserted, {result['updated']} updated, "
            f"{result['rows']} total processed."
        )
        finish_etl_run(
            run_id,
            "SUCCESS",
            date_from=date_from,
            date_to=date_to,
            rows_read=rows_read,
            rows_loaded=result["rows"],
            rows_inserted=result["inserted"],
            rows_updated=result["updated"],
        )
        return result
    except Exception as exc:
        finish_etl_run(
            run_id,
            "FAILED",
            date_from=date_from,
            date_to=date_to,
            rows_read=rows_read,
            error_message=str(exc),
        )
        raise


def run_data_dump_pipeline(
    excel_file: str,
    downloaded_from_portal: bool = False,
    should_run_validation: bool = True,
    start_date: date | None = None,
    end_date: date | None = None,
    source_mode: str | None = None,
    logger=print,
) -> dict:
    source_mode = source_mode or ("portal" if downloaded_from_portal else "local")
    run_id = start_etl_run(
        "DATA_DUMP",
        source_mode,
        excel_file,
        validation_enabled=should_run_validation,
    )
    date_from = None
    date_to = None
    rows_read = None

    try:
        if start_date and end_date and start_date > end_date:
            raise ValueError("--start-date cannot be after --end-date.")

        logger(f"\nSource file: {excel_file}")
        logger("\nReading Excel file once...")
        source_df = prepare_source_dataframe(excel_file)
        logger(f"Source rows loaded in memory: {len(source_df)}")

        source_df = filter_source_dataframe_by_date(source_df, start_date, end_date)
        rows_read = len(source_df)
        if not source_df.empty:
            visit_dates = source_df["date"].dt.date
            date_from = visit_dates.min()
            date_to = visit_dates.max()

        if start_date or end_date:
            logger(
                "Rows after date filter "
                f"{start_date or 'first available'} to {end_date or 'last available'}: "
                f"{len(source_df)}"
            )

        if source_df.empty:
            logger("\nProcess stopped: no rows matched the selected date filter.")
            finish_etl_run(
                run_id,
                "SUCCESS",
                rows_read=0,
                rows_loaded=0,
                rows_inserted=0,
                rows_updated=0,
                affected_visits=0,
                survey_responses=0,
            )
            return {"rows": 0, "affected_visit_ids": []}

        logger("\nRunning core ETL...")
        etl_result = run_etl(source_df, logger=logger)
        affected_visit_ids = etl_result.get("affected_visit_ids", [])
        logger("\nCore ETL finished.")
        logger(f"Affected visits in uploaded file: {len(affected_visit_ids)}")
        cleanup_result = etl_result.get("idempotency_cleanup", {})
        if cleanup_result:
            logger(
                "Idempotency cleanup summary: "
                f"{cleanup_result.get('survey_responses_deleted', 0)} old survey_responses rows deleted, "
                f"{cleanup_result.get('task_rows_deleted', 0)} old task rows deleted."
            )

        logger("\nFetching visits lookup for survey_responses...")
        visit_lookup_df = fetch_visit_lookup_dataframe()

        logger("\nBuilding survey_responses dataframe...")
        df = build_survey_responses_dataframe(source_df, visit_lookup_df)
        logger(f"Transformed rows ready for survey_responses load: {len(df)}")

        if df.empty:
            logger("\nProcess stopped: no valid rows after transformation.")
            finish_etl_run(
                run_id,
                "SUCCESS",
                date_from=date_from,
                date_to=date_to,
                rows_read=rows_read,
                rows_loaded=etl_result.get("rows"),
                rows_inserted=0,
                rows_updated=len(affected_visit_ids),
                affected_visits=len(affected_visit_ids),
                survey_responses=0,
            )
            return {**etl_result, "survey_responses": 0}

        logger("\nLoading survey_responses...")
        inserted_rows = load_survey_responses(df, cleanup_existing=False, logger=logger)
        logger(f"Inserted rows into survey_responses: {inserted_rows}")

        if should_run_validation:
            logger("\nRunning database validations...")
            run_validation(target_visit_ids=affected_visit_ids)
        else:
            logger("\nValidation skipped for history/backfill load.")

        finish_etl_run(
            run_id,
            "SUCCESS",
            date_from=date_from,
            date_to=date_to,
            rows_read=rows_read,
            rows_loaded=etl_result.get("rows"),
            rows_inserted=inserted_rows,
            rows_updated=len(affected_visit_ids),
            affected_visits=len(affected_visit_ids),
            survey_responses=inserted_rows,
        )
        return {**etl_result, "survey_responses": inserted_rows}
    except Exception as exc:
        finish_etl_run(
            run_id,
            "FAILED",
            date_from=date_from,
            date_to=date_to,
            rows_read=rows_read,
            error_message=str(exc),
        )
        raise
