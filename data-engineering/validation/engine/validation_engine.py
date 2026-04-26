from validation.rules.osa_unusual_non import run_osa_unusual_non_validation
from validation.rules.gps_inconsistent_checkin_same_store_month import (
    run_gps_inconsistent_checkin_same_store_month_validation,
)


def run_all_validations(run_id: int) -> dict:
    results = {}

    osa_unusual_non_count = run_osa_unusual_non_validation(run_id)
    results["OSA_UNUSUAL_NON_BY_BANNER"] = osa_unusual_non_count

    gps_inconsistent_count = run_gps_inconsistent_checkin_same_store_month_validation(run_id)
    results["GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH"] = gps_inconsistent_count

    return results