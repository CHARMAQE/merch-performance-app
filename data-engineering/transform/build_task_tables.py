import pandas as pd

from transform.etl_constants import (
    LOCATION_TABLES,
    TABLES_WITH_PRODUCT,
    TABLES_WITH_TASK,
    TABLES_WITH_TITLE,
    TASK_TABLE_MAP,
    TITLE_TABLE_MAP,
)
from transform.etl_helpers import clean_text, question_to_column


def get_task_rows(df):
    df = df.copy()
    df["_target_table"] = None

    for task_name, table_name in TASK_TABLE_MAP.items():
        mask = df["task"].astype(str).str.strip() == task_name
        df.loc[mask, "_target_table"] = table_name

    unmatched = df["_target_table"].isna()
    for title_key, table_name in TITLE_TABLE_MAP.items():
        mask = unmatched & df["title"].astype(str).str.upper().str.contains(title_key, na=False)
        df.loc[mask, "_target_table"] = table_name
        unmatched = df["_target_table"].isna()

    return df[df["_target_table"].notna()].copy()


def build_question_column_map(questions):
    col_map = {}
    used_cols = set()

    for q in questions:
        col = question_to_column(q)
        base = col
        i = 2

        while col in used_cols:
            suffix = f"_{i}"
            col = f"{base[:64 - len(suffix)]}{suffix}"
            i += 1

        col_map[q] = col
        used_cols.add(col)

    return col_map


def build_tagged_task_dataframe(df, visit_map, prod_map):
    tagged_df = get_task_rows(df)

    tagged_df["_visit_id"] = tagged_df.apply(
        lambda r: visit_map.get(
            (
                str(r["date"].date()) if pd.notna(r["date"]) else None,
                clean_text(r["employeecode"]),
                clean_text(r["storecode"]),
            )
        ),
        axis=1,
    )

    tagged_df["_product_id"] = tagged_df["productcode"].apply(lambda x: prod_map.get(clean_text(x)))
    tagged_df = tagged_df[tagged_df["_visit_id"].notna()].copy()

    return tagged_df


def build_task_table_batches(tagged_df):
    batches = []

    if tagged_df.empty:
        return batches

    for table_name in sorted(tagged_df["_target_table"].dropna().unique()):
        table_df = tagged_df[tagged_df["_target_table"] == table_name].copy()

        if table_name == "task_secondary_placement":
            table_df = table_df[
                ~table_df["task"].astype(str).str.upper().str.contains("GLACE", na=False)
            ]

        if table_df.empty:
            continue

        batch = {
            "table_name": table_name,
            "table_df": table_df,
            "has_product": table_name in TABLES_WITH_PRODUCT,
            "has_title": table_name in TABLES_WITH_TITLE,
            "has_task": table_name in TABLES_WITH_TASK,
            "has_location_meta": table_name in LOCATION_TABLES,
        }

        if table_name != "task_sos":
            questions = table_df["question"].dropna().astype(str).str.strip().unique()
            questions = sorted([q for q in questions if q])

            batch["questions"] = questions
            batch["question_col_map"] = build_question_column_map(questions)

        batches.append(batch)

    return batches
