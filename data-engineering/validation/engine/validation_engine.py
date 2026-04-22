from validation.rules.osa_unusual_non import run_osa_unusual_non_validation


def run_all_validations(run_id: int) -> dict:
    results = {}

    osa_unusual_non_count = run_osa_unusual_non_validation(run_id)
    results["OSA_UNUSUAL_NON_BY_BANNER"] = osa_unusual_non_count

    return results