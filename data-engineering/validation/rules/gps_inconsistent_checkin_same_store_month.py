"""
Validation rule: GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH

Business objective
------------------
Detect suspicious merchandising visits where the same merchandiser reports the
same store during the same month, but one or more check-in GPS positions are far
from the merchandiser's normal GPS cluster for that store.

Why this is useful
------------------
In GT/retail execution, a merchandiser can sometimes visit/report the wrong
store, or submit data while physically located near another store. This rule does
not need official store coordinates: it compares the merchandiser's repeated
check-ins for the same store and month against the median GPS point.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import radians, sin, cos, asin, sqrt
from statistics import median
from typing import Iterable

import mysql.connector

from config.db_config import DB_CONFIG


RULE_CODE = "GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH"
MIN_VISITS_PER_MONTH = 3
WARNING_DISTANCE_METERS = 300
HIGH_DISTANCE_METERS = 700


@dataclass(frozen=True)
class VisitGps:
    visit_id: int
    visit_date: object
    employee_code: str
    username: str | None
    store_code: str
    store_name: str | None
    year_num: int
    month_num: int
    latitude: float
    longitude: float


def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in meters between two GPS points."""
    earth_radius_m = 6_371_000
    phi1, phi2 = radians(lat1), radians(lat2)
    d_phi = radians(lat2 - lat1)
    d_lambda = radians(lon2 - lon1)

    a = sin(d_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(d_lambda / 2) ** 2
    c = 2 * asin(sqrt(a))
    return earth_radius_m * c


def _banner_case_sql() -> str:
    return """
    CASE
        WHEN UPPER(TRIM(s.store_name)) LIKE 'MARJANE MARKET%' THEN 'MARJANE MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'ACIMA%' THEN 'MARJANE MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'MARJANE%' THEN 'MARJANE'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'CARREFOUR MARKET%' THEN 'CARREFOUR MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'CARREFOUR%' THEN 'CARREFOUR'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'ATACADAO%' THEN 'ATACADAO'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'ATTACADAO%' THEN 'ATACADAO'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'ASWAK ASSALAM%' THEN 'ASWAK ASSALAM'
        ELSE 'OTHER'
    END
    """


def _fetch_visit_gps_rows(cursor) -> list[VisitGps]:
    query = f"""
    SELECT
        v.visit_id,
        v.visit_date,
        e.employee_code,
        e.username,
        s.store_code,
        s.store_name,
        YEAR(v.visit_date) AS year_num,
        MONTH(v.visit_date) AS month_num,
        CAST(v.latitude AS DECIMAL(10,6)) AS latitude,
        CAST(v.longitude AS DECIMAL(10,6)) AS longitude,
        {_banner_case_sql()} AS banner
    FROM visits v
    JOIN employees e ON e.employee_id = v.employee_id
    JOIN stores s ON s.store_id = v.store_id
    WHERE v.visit_date IS NOT NULL
      AND v.latitude IS NOT NULL
      AND v.longitude IS NOT NULL
      AND v.latitude <> 0
      AND v.longitude <> 0
    ORDER BY e.employee_code, s.store_code, YEAR(v.visit_date), MONTH(v.visit_date), v.visit_date, v.visit_id
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    result: list[VisitGps] = []
    for row in rows:
        result.append(
            VisitGps(
                visit_id=int(row["visit_id"]),
                visit_date=row["visit_date"],
                employee_code=str(row["employee_code"]),
                username=row.get("username"),
                store_code=str(row["store_code"]),
                store_name=row.get("store_name"),
                year_num=int(row["year_num"]),
                month_num=int(row["month_num"]),
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
            )
        )
    return result


def _group_rows(rows: Iterable[VisitGps]) -> dict[tuple[str, str, int, int], list[VisitGps]]:
    groups: dict[tuple[str, str, int, int], list[VisitGps]] = defaultdict(list)
    for row in rows:
        key = (row.employee_code, row.store_code, row.year_num, row.month_num)
        groups[key].append(row)
    return groups


def run_gps_inconsistent_checkin_same_store_month_validation(_run_id: int) -> int:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    # Because the current validation_results table does not yet store run_id,
    # we clean only this rule before inserting fresh results.
    cursor.execute("DELETE FROM validation_results WHERE rule_code = %s", (RULE_CODE,))

    rows = _fetch_visit_gps_rows(cursor)
    groups = _group_rows(rows)

    insert_sql = """
    INSERT INTO validation_results (
        rule_code,
        visit_id,
        store_code,
        employee_code,
        product_code,
        banner,
        question,
        response,
        message,
        no_count,
        yes_count,
        total_answers,
        availability_rate
    )
    VALUES (
        %(rule_code)s,
        %(visit_id)s,
        %(store_code)s,
        %(employee_code)s,
        NULL,
        %(banner)s,
        %(question)s,
        %(response)s,
        %(message)s,
        NULL,
        NULL,
        %(total_answers)s,
        NULL
    )
    """

    payloads = []

    for (_employee_code, _store_code, year_num, month_num), visits in groups.items():
        if len(visits) < MIN_VISITS_PER_MONTH:
            continue

        median_lat = median(v.latitude for v in visits)
        median_lon = median(v.longitude for v in visits)

        for visit in visits:
            distance_m = _haversine_meters(
                visit.latitude,
                visit.longitude,
                median_lat,
                median_lon,
            )

            if distance_m <= WARNING_DISTANCE_METERS:
                continue

            severity = "HIGH" if distance_m >= HIGH_DISTANCE_METERS else "MEDIUM"
            distance_km = distance_m / 1000

            payloads.append(
                {
                    "rule_code": RULE_CODE,
                    "visit_id": visit.visit_id,
                    "store_code": visit.store_code,
                    "employee_code": visit.employee_code,
                    "banner": severity,  # temporary use until a severity column is added
                    "question": "Monthly GPS consistency check for repeated visits to the same store",
                    "response": (
                        f"visit_gps=({visit.latitude:.6f},{visit.longitude:.6f}); "
                        f"normal_monthly_gps=({median_lat:.6f},{median_lon:.6f})"
                    ),
                    "message": (
                        f"{severity}: GPS check-in for {visit.employee_code} at store "
                        f"{visit.store_code} ({visit.store_name or 'Unknown store'}) on {visit.visit_date} "
                        f"is {distance_m:.0f} meters ({distance_km:.2f} km) away from the normal monthly GPS zone. "
                        f"This store was visited {len(visits)} times by the same merchandiser in "
                        f"{year_num}-{month_num:02d}. This may indicate wrong-store execution or a visit submitted near another GT store."
                    ),
                    "total_answers": len(visits),
                }
            )

    if payloads:
        cursor.executemany(insert_sql, payloads)

    conn.commit()
    inserted = len(payloads)
    cursor.close()
    conn.close()
    return inserted
