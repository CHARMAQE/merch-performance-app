import pandas as pd

from transform.etl_helpers import clean_float, clean_text


def prepare_source_dataframe(excel_file):
    df = pd.read_excel(excel_file)
    df.columns = df.columns.str.strip().str.lower()

    df["date"] = pd.to_datetime(df["dateid"].astype(str), format="%Y%m%d", errors="coerce")
    df["responsedate"] = pd.to_datetime(
        pd.to_numeric(df["responsedate"], errors="coerce"),
        unit="D",
        origin="1899-12-30",
        errors="coerce",
    )

    return df


def build_employees_dataframe(df):
    out = df[["employeecode", "username"]].copy()

    out["employee_code"] = out["employeecode"].apply(clean_text)
    out["username"] = out["username"].apply(clean_text)

    out = out[["employee_code", "username"]]
    out = out[out["employee_code"].notna()].drop_duplicates(subset="employee_code").reset_index(drop=True)

    return out


def build_stores_dataframe(df):
    out = df[
        [
            "storecode",
            "storename",
            "storecity",
            "storestate",
            "storeregion",
            "storeformat",
        ]
    ].copy()

    out["store_code"] = out["storecode"].apply(clean_text)
    out["store_name"] = out["storename"].apply(clean_text)
    out["store_city"] = out["storecity"].apply(clean_text)
    out["store_state"] = out["storestate"].apply(clean_text)
    out["store_region"] = out["storeregion"].apply(clean_text)
    out["store_format"] = out["storeformat"].apply(clean_text)

    out = out[
        [
            "store_code",
            "store_name",
            "store_city",
            "store_state",
            "store_region",
            "store_format",
        ]
    ]
    out = out[out["store_code"].notna()].drop_duplicates(subset="store_code").reset_index(drop=True)

    return out


def build_products_dataframe(df):
    out = df[
        [
            "productcode",
            "productbarcode",
            "productdescription",
            "brandname",
            "category",
            "subcategory",
        ]
    ].copy()

    out["product_code"] = out["productcode"].apply(clean_text)
    out["barcode"] = out["productbarcode"].apply(clean_text)
    out["product_description"] = out["productdescription"].apply(clean_text)
    out["brand"] = out["brandname"].apply(clean_text)
    out["category"] = out["category"].apply(clean_text)
    out["sub_category"] = out["subcategory"].apply(clean_text)

    out = out[
        [
            "product_code",
            "barcode",
            "product_description",
            "brand",
            "category",
            "sub_category",
        ]
    ]
    out = out[out["product_code"].notna()].drop_duplicates(subset="product_code").reset_index(drop=True)

    return out


def build_visits_dataframe(df):
    out = df[
        [
            "date",
            "year",
            "month",
            "employeecode",
            "storecode",
            "latitude",
            "longitude",
        ]
    ].copy()

    out["visit_date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["employee_code"] = out["employeecode"].apply(clean_text)
    out["store_code"] = out["storecode"].apply(clean_text)
    out["month"] = out["month"].apply(clean_text)
    out["latitude"] = out["latitude"].apply(clean_float)
    out["longitude"] = out["longitude"].apply(clean_float)

    out["map_link"] = out.apply(
        lambda r: (
            f"https://www.google.com/maps?q={r['latitude']},{r['longitude']}"
            if r["latitude"] is not None and r["longitude"] is not None
            else None
        ),
        axis=1,
    )

    out = out[
        [
            "visit_date",
            "year",
            "month",
            "employee_code",
            "store_code",
            "latitude",
            "longitude",
            "map_link",
        ]
    ]

    out = out[
        out["visit_date"].notna()
        & out["employee_code"].notna()
        & out["store_code"].notna()
    ]

    out = out.drop_duplicates(subset=["visit_date", "employee_code", "store_code"]).reset_index(drop=True)

    return out
