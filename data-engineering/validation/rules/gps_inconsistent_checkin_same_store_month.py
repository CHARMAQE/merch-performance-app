from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import json
from math import asin, cos, radians, sin, sqrt
from statistics import median
from typing import Iterable

import mysql.connector

from config.db_config import DB_CONFIG


RULE_CODE = "GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH"
RULE_NAME = "GPS inconsistent check-in same store month"
RULE_DESCRIPTION = (
    "Flags repeated same-store monthly visits whose GPS position is far from "
    "the merchandiser's normal GPS cluster for that store."
)
SOURCE_TABLE = "visits"
SEVERITY = "MEDIUM"
ENTITY_TYPE = "visit"

MIN_VISITS_PER_MONTH = 3
WARNING_DISTANCE_METERS = 1000
HIGH_DISTANCE_METERS = 2000
RULE_QUESTION = "Monthly GPS consistency check for repeated visits to the same store"


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
    earth_radius_m = 6_371_000
    phi1, phi2 = radians(lat1), radians(lat2)
    d_phi = radians(lat2 - lat1)
    d_lambda = radians(lon2 - lon1)

    a = sin(d_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(d_lambda / 2) ** 2
    c = 2 * asin(sqrt(a))
    return earth_radius_m * c


def _fetch_visit_gps_rows(cursor) -> list[VisitGps]:
    cursor.execute(
        """
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
            CAST(v.longitude AS DECIMAL(10,6)) AS longitude
        FROM visits v
        JOIN employees e ON e.employee_id = v.employee_id
        JOIN stores s ON s.store_id = v.store_id
        WHERE v.visit_date IS NOT NULL
          AND v.latitude IS NOT NULL
          AND v.longitude IS NOT NULL
          AND v.latitude <> 0
          AND v.longitude <> 0
        ORDER BY
            e.employee_code,
            s.store_code,
            YEAR(v.visit_date),
            MONTH(v.visit_date),
            v.visit_date,
            v.visit_id
        """
    )
    rows = cursor.fetchall()

    visits = []
    for row in rows:
        visits.append(
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
    return visits


def _group_rows(rows: Iterable[VisitGps]) -> dict[tuple[str, str, int, int], list[VisitGps]]:
    groups: dict[tuple[str, str, int, int], list[VisitGps]] = defaultdict(list)
    for row in rows:
        groups[(row.employee_code, row.store_code, row.year_num, row.month_num)].append(row)
    return groups


def _normalize_target_visit_ids(target_visit_ids: list[int] | None) -> set[int] | None:
    if target_visit_ids is None:
        return None

    return {int(visit_id) for visit_id in target_visit_ids}


def run(run_id: int, target_visit_ids: list[int] | None = None) -> int:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    normalized_target_visit_ids = _normalize_target_visit_ids(target_visit_ids)

    try:
        if normalized_target_visit_ids == set():
            return 0

        rows = _fetch_visit_gps_rows(cursor)
        groups = _group_rows(rows)

        insert_sql = """
        INSERT INTO validation_results (
            run_id,
            rule_code,
            entity_type,
            entity_id,
            visit_id,
            store_code,
            employee_code,
            product_code,
            question,
            actual_value,
            expected_value,
            metric_value,
            message,
            severity,
            details_json
        )
        VALUES (
            %(run_id)s,
            %(rule_code)s,
            %(entity_type)s,
            %(entity_id)s,
            %(visit_id)s,
            %(store_code)s,
            %(employee_code)s,
            NULL,
            %(question)s,
            %(actual_value)s,
            %(expected_value)s,
            %(metric_value)s,
            %(message)s,
            %(severity)s,
            %(details_json)s
        )
        """

        payloads = []
        for (_employee_code, _store_code, year_num, month_num), visits in groups.items():
            if len(visits) < MIN_VISITS_PER_MONTH:
                continue

            median_lat = median(v.latitude for v in visits)
            median_lon = median(v.longitude for v in visits)

            for visit in visits:
                if normalized_target_visit_ids is not None and visit.visit_id not in normalized_target_visit_ids:
                    continue

                distance_m = _haversine_meters(
                    visit.latitude,
                    visit.longitude,
                    median_lat,
                    median_lon,
                )

                if distance_m <= WARNING_DISTANCE_METERS:
                    continue

                row_severity = "HIGH" if distance_m >= HIGH_DISTANCE_METERS else "MEDIUM"
                distance_km = distance_m / 1000

                details = {
                    "visit_date": str(visit.visit_date),
                    "store_name": visit.store_name,
                    "username": visit.username,
                    "year_num": year_num,
                    "month_num": month_num,
                    "visit_latitude": visit.latitude,
                    "visit_longitude": visit.longitude,
                    "median_latitude": median_lat,
                    "median_longitude": median_lon,
                    "distance_meters": round(distance_m, 2),
                    "distance_km": round(distance_km, 3),
                    "total_visits_in_group": len(visits),
                    "warning_threshold_meters": WARNING_DISTANCE_METERS,
                    "high_threshold_meters": HIGH_DISTANCE_METERS,
                }

                payloads.append(
                    {
                        "run_id": run_id,
                        "rule_code": RULE_CODE,
                        "entity_type": ENTITY_TYPE,
                        "entity_id": str(visit.visit_id),
                        "visit_id": visit.visit_id,
                        "store_code": visit.store_code,
                        "employee_code": visit.employee_code,
                        "question": RULE_QUESTION,
                        "actual_value": (
                            f"visit_gps=({visit.latitude:.6f},{visit.longitude:.6f})"
                        ),
                        "expected_value": (
                            f"monthly_cluster_gps=({median_lat:.6f},{median_lon:.6f})"
                        ),
                        "metric_value": round(distance_m, 2),
                        "message": (
                            f"{row_severity}: GPS check-in for {visit.employee_code} at store "
                            f"{visit.store_code} ({visit.store_name or 'Unknown store'}) on {visit.visit_date} "
                            f"is {distance_m:.0f} meters ({distance_km:.2f} km) away from the normal monthly GPS zone. "
                            f"This store was visited {len(visits)} times by the same merchandiser in "
                            f"{year_num}-{month_num:02d}."
                        ),
                        "severity": row_severity,
                        "details_json": json.dumps(details),
                    }
                )

        if payloads:
            cursor.executemany(insert_sql, payloads)

        conn.commit()
        return len(payloads)

    finally:
        cursor.close()
        conn.close()


def run_gps_inconsistent_checkin_same_store_month_validation(run_id: int, target_visit_ids: list[int] | None = None) -> int:
    return run(run_id, target_visit_ids=target_visit_ids)
