import re

import pandas as pd

from transform.etl_helpers import clean_float, clean_text


COVERAGE_COLUMNS = [
    "visit_date",
    "year",
    "month",
    "dateid",
    "employee_code",
    "username",
    "role",
    "l1name",
    "l2name",
    "l3name",
    "store_code",
    "store_name",
    "store_region",
    "store_state",
    "store_city",
    "store_format",
    "call_cycle_type",
    "call_status",
    "is_planned",
    "is_adhoc",
    "is_done",
    "rejection",
    "deviation",
    "not_visited",
    "task_assigned",
    "task_done",
    "task_per",
    "master_latitude",
    "master_longitude",
    "start_time",
    "start_latitude",
    "start_longitude",
    "start_distance_meters",
    "end_time",
    "end_latitude",
    "end_longitude",
    "end_distance_meters",
    "time_mm",
    "time_hh",
    "reason",
    "user_attendance",
    "superior_attendance",
    "final_user_attendance",
]

BOOLEAN_COLUMNS = ["is_planned", "is_done", "rejection", "deviation", "not_visited"]
TEXT_COLUMNS = [
    "employee_code",
    "username",
    "role",
    "l1name",
    "l2name",
    "l3name",
    "store_code",
    "store_name",
    "store_region",
    "store_state",
    "store_city",
    "store_format",
    "call_cycle_type",
    "call_status",
    "reason",
    "user_attendance",
    "superior_attendance",
    "final_user_attendance",
]
FLOAT_COLUMNS = [
    "task_per",
    "master_latitude",
    "master_longitude",
    "start_latitude",
    "start_longitude",
    "start_distance_meters",
    "end_latitude",
    "end_longitude",
    "end_distance_meters",
    "time_hh",
]
INTEGER_COLUMNS = ["year", "dateid", "task_assigned", "task_done", "time_mm"]
DATETIME_COLUMNS = ["start_time", "end_time"]

ALIASES = {
    "date": "visit_date",
    "date_id": "dateid",
    "employee": "employee_code",
    "employee_id": "employee_code",
    "employeecode": "employee_code",
    "user_name": "username",
    "store": "store_code",
    "storecode": "store_code",
    "storename": "store_name",
    "storeregion": "store_region",
    "storestate": "store_state",
    "storecity": "store_city",
    "storeformat": "store_format",
    "callcycletype": "call_cycle_type",
    "call_cycle": "call_cycle_type",
    "callstatus": "call_status",
    "planned": "is_planned",
    "isplanned": "is_planned",
    "adhoc": "is_adhoc",
    "isadhoc": "is_adhoc",
    "done": "is_done",
    "isdone": "is_done",
    "rejected": "rejection",
    "deviated": "deviation",
    "notvisited": "not_visited",
    "taskassigned": "task_assigned",
    "taskdone": "task_done",
    "taskpercentage": "task_per",
    "task_percent": "task_per",
    "masterlat": "master_latitude",
    "masterlatitude": "master_latitude",
    "masterlon": "master_longitude",
    "masterlng": "master_longitude",
    "masterlongitude": "master_longitude",
    "starttime": "start_time",
    "startlat": "start_latitude",
    "startlatitude": "start_latitude",
    "startlon": "start_longitude",
    "startlng": "start_longitude",
    "startlongitude": "start_longitude",
    "startdistance": "start_distance_meters",
    "startdistancemeters": "start_distance_meters",
    "endtime": "end_time",
    "endlat": "end_latitude",
    "endlatitude": "end_latitude",
    "endlon": "end_longitude",
    "endlng": "end_longitude",
    "endlongitude": "end_longitude",
    "enddistance": "end_distance_meters",
    "enddistancemeters": "end_distance_meters",
    "timemm": "time_mm",
    "timehh": "time_hh",
    "userattendance": "user_attendance",
    "superiorattendance": "superior_attendance",
    "finaluserattendance": "final_user_attendance",
}


def to_snake_case(value) -> str:
    text = str(value).strip()
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_").lower()
    return ALIASES.get(text, text)


def read_coverage_excel(excel_file) -> pd.DataFrame:
    df = pd.read_excel(excel_file)
    df.columns = [to_snake_case(col) for col in df.columns]
    return df


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    for col in df.columns:
        if col not in out:
            out[col] = df[col]
        else:
            out[col] = out[col].combine_first(df[col])

    return out


def _parse_visit_date(df: pd.DataFrame) -> pd.Series:
    if "visit_date" in df:
        parsed = df["visit_date"].apply(_parse_datetime)
        parsed = pd.to_datetime(parsed, errors="coerce")
        if parsed.notna().any():
            return parsed.dt.date

    if "dateid" in df:
        dateid = df["dateid"].apply(clean_text)
        parsed = pd.to_datetime(dateid, format="%Y%m%d", errors="coerce")
        return parsed.dt.date

    raise ValueError("Coverage file must contain visit_date/date or dateid.")


def _to_bool_int(value):
    if pd.isna(value):
        return 0

    if isinstance(value, bool):
        return int(value)

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "done", "visited", "planned", "deviation", "rejected"}:
        return 1
    if text in {"0", "false", "no", "n", "not done", "not visited", "none", ""}:
        return 0

    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return 0
    return 1 if numeric != 0 else 0


def _to_int(value):
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return int(numeric)


def _parse_datetime(value):
    if pd.isna(value):
        return None

    numeric = pd.to_numeric(value, errors="coerce")
    if not pd.isna(numeric) and 10000101 <= numeric <= 99991231:
        parsed = pd.to_datetime(str(int(numeric)), format="%Y%m%d", errors="coerce")
        if not pd.isna(parsed):
            return parsed.to_pydatetime()

    if not pd.isna(numeric) and 20000 <= numeric <= 60000:
        parsed = pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")
        if not pd.isna(parsed):
            return parsed.to_pydatetime()

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        if pd.isna(numeric):
            return None
        parsed = pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")

    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def build_coverage_dataframe(source_df: pd.DataFrame) -> pd.DataFrame:
    df = source_df.copy()
    df.columns = [to_snake_case(col) for col in df.columns]
    df = _coalesce_duplicate_columns(df)

    for col in COVERAGE_COLUMNS:
        if col not in df:
            df[col] = None

    df["visit_date"] = _parse_visit_date(df)
    visit_dates = pd.to_datetime(df["visit_date"], errors="coerce")

    if df["year"].isna().all():
        df["year"] = visit_dates.dt.year
    if df["month"].isna().all():
        df["month"] = visit_dates.dt.month_name()

    df["dateid"] = visit_dates.dt.strftime("%Y%m%d")

    for col in TEXT_COLUMNS:
        df[col] = df[col].apply(clean_text)

    for col in BOOLEAN_COLUMNS:
        df[col] = df[col].apply(_to_bool_int)

    if "is_adhoc" in df:
        df["is_adhoc"] = df["is_adhoc"].apply(_to_bool_int)

    for col in FLOAT_COLUMNS:
        df[col] = df[col].apply(clean_float)

    for col in INTEGER_COLUMNS:
        df[col] = df[col].apply(_to_int)

    for col in DATETIME_COLUMNS:
        df[col] = df[col].apply(_parse_datetime)

    df = df[COVERAGE_COLUMNS]
    df = df[
        df["visit_date"].notna()
        & df["employee_code"].notna()
        & df["store_code"].notna()
    ]
    df = df.drop_duplicates(
        subset=["visit_date", "employee_code", "store_code"],
        keep="last",
    ).reset_index(drop=True)

    return df


def prepare_coverage_dataframe(excel_file) -> pd.DataFrame:
    return build_coverage_dataframe(read_coverage_excel(excel_file))
