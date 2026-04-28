import pandas as pd

from transform.etl_helpers import clean_float, clean_text


SURVEY_RESPONSE_COLUMNS = [
    "visit_id",
    "employee_code",
    "store_code",
    "product_code",
    "task",
    "title",
    "question",
    "response",
    "response_datetime",
    "latitude",
    "longitude",
]


def build_survey_responses_dataframe(source_df: pd.DataFrame, visit_lookup_df: pd.DataFrame) -> pd.DataFrame:
    if source_df is None or source_df.empty:
        return pd.DataFrame(columns=SURVEY_RESPONSE_COLUMNS)

    df = source_df.copy()

    if "storeformat" in df.columns:
        df = df[
            ~df["storeformat"]
            .astype(str)
            .str.strip()
            .str.upper()
            .isin(["GROCERY", "ICE CREAM"])
        ].copy()

    if "date" in df.columns:
        df["visit_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    else:
        df["visit_date"] = pd.to_datetime(
            df["dateid"].astype(str),
            format="%Y%m%d",
            errors="coerce",
        ).dt.date

    df["response_datetime"] = pd.to_datetime(
        df["responsedate"],
        errors="coerce",
    ).dt.round("s")

    df["employee_code"] = df["employeecode"].apply(clean_text)
    df["store_code"] = df["storecode"].apply(clean_text)
    df["product_code"] = df["productcode"].apply(clean_text)
    df["task"] = df["task"].apply(clean_text)
    df["title"] = df["title"].apply(clean_text)
    df["question"] = df["question"].apply(clean_text)
    df["response"] = df["response"].apply(clean_text)
    df["latitude"] = df["latitude"].apply(clean_float)
    df["longitude"] = df["longitude"].apply(clean_float)

    if visit_lookup_df is None or visit_lookup_df.empty:
        return pd.DataFrame(columns=SURVEY_RESPONSE_COLUMNS)

    visit_lookup_df = visit_lookup_df.copy()
    visit_lookup_df["visit_date"] = pd.to_datetime(
        visit_lookup_df["visit_date"],
        errors="coerce",
    ).dt.date
    visit_lookup_df["employee_code"] = visit_lookup_df["employee_code"].apply(clean_text)
    visit_lookup_df["store_code"] = visit_lookup_df["store_code"].apply(clean_text)

    merged = df.merge(
        visit_lookup_df[["visit_id", "visit_date", "employee_code", "store_code"]],
        how="left",
        on=["visit_date", "employee_code", "store_code"],
    )

    out = merged[
        [
            "visit_id",
            "employee_code",
            "store_code",
            "product_code",
            "task",
            "title",
            "question",
            "response",
            "response_datetime",
            "latitude",
            "longitude",
        ]
    ].copy()

    out = out[out["visit_id"].notna()].copy()
    out = out.drop_duplicates().copy()
    out = out.where(pd.notna(out), None)
    out = out.reset_index(drop=True)

    return out
