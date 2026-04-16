import re
import pandas as pd


def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def clean_float(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def question_to_column(question_text):
    """Convert a question string into a valid MySQL column name."""
    col = str(question_text).strip().lower()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = col.strip("_")
    if not col:
        col = "unknown"
    col = "q_" + col[:50]
    return col