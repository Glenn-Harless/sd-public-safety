"""Shared query layer for API and MCP server."""

from __future__ import annotations

from pathlib import Path

import duckdb

_AGG = Path(__file__).resolve().parent.parent / "data" / "aggregated"
_PROC = Path(__file__).resolve().parent.parent / "data" / "processed"

# Aggregation parquet paths
_YEARLY_SUMMARY = _AGG / "yearly_summary.parquet"
_CRIME_MONTHLY = _AGG / "crime_overview_monthly.parquet"
_CRIME_BY_TYPE = _AGG / "crime_by_type.parquet"
_CRIME_BY_ZIP = _AGG / "crime_by_zip.parquet"
_CRIME_BY_AGENCY = _AGG / "crime_by_agency.parquet"
_CRIME_BY_CITY = _AGG / "crime_by_city.parquet"
_VICTIM_DEMO = _AGG / "victim_demographics.parquet"
_DV = _AGG / "domestic_violence.parquet"
_TEMPORAL = _AGG / "temporal_patterns.parquet"
_MAP_POINTS = _AGG / "map_points.parquet"
_ARRESTS = _AGG / "arrests_by_type.parquet"
_CFS_MONTHLY = _AGG / "cfs_monthly.parquet"
_CFS_BY_BEAT = _AGG / "cfs_by_beat.parquet"
_CFS_TEMPORAL = _AGG / "cfs_temporal.parquet"


def _run(sql: str) -> list[dict]:
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchdf().to_dict(orient="records")
    finally:
        con.close()


def _q(where: str, condition: str) -> str:
    if not where:
        return f"WHERE {condition}"
    return f"{where} AND {condition}"


def _where(
    year_min: int | None = None,
    year_max: int | None = None,
    agency: str | None = None,
    crime_against: str | None = None,
    city: str | None = None,
    *,
    has_agency: bool = True,
    has_crime_against: bool = True,
    has_city: bool = False,
    year_col: str = "year",
) -> str:
    w = ""
    if year_min is not None:
        w = _q(w, f"{year_col} >= {year_min}")
    if year_max is not None:
        w = _q(w, f"{year_col} <= {year_max}")
    if agency and has_agency:
        w = _q(w, f"agency_short = '{agency.replace(chr(39), chr(39)*2)}'")
    if crime_against and has_crime_against:
        w = _q(w, f"crime_against = '{crime_against.replace(chr(39), chr(39)*2)}'")
    if city and has_city:
        w = _q(w, f"city = '{city.replace(chr(39), chr(39)*2)}'")
    return w


# ── Filter options ────────────────────────────────────────────────────

def get_filter_options() -> dict:
    """Get available filter values for all datasets."""
    con = duckdb.connect()
    try:
        years = [r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM '{_CRIME_BY_AGENCY}' ORDER BY year"
        ).fetchall()]
        agencies = [r[0] for r in con.execute(
            f"SELECT DISTINCT agency_short FROM '{_CRIME_BY_AGENCY}' ORDER BY agency_short"
        ).fetchall()]
        categories = [r[0] for r in con.execute(
            f"SELECT DISTINCT crime_against FROM '{_CRIME_BY_AGENCY}' WHERE crime_against IS NOT NULL ORDER BY crime_against"
        ).fetchall()]
        cities = [r[0] for r in con.execute(
            f"SELECT DISTINCT city FROM '{_CRIME_BY_CITY}' WHERE city IS NOT NULL ORDER BY city"
        ).fetchall()]
        return {
            "years": years,
            "agencies": agencies,
            "crime_categories": categories,
            "cities": cities,
        }
    finally:
        con.close()


# ── Overview / KPIs ──────────────────────────────────────────────────

def get_overview(
    year_min: int = 2021, year_max: int = 2025,
    agency: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, agency)
    return _run(f"SELECT * FROM '{_YEARLY_SUMMARY}' {w} ORDER BY year")


# ── Crime trends ─────────────────────────────────────────────────────

def get_trends(
    year_min: int = 2021, year_max: int = 2025,
    agency: str | None = None, crime_against: str | None = None,
) -> list[dict]:
    # crime_overview_monthly has no "year" col — derive from month_start
    w = _where(year_min, year_max, agency, crime_against, year_col="YEAR(month_start)")
    return _run(f"""
        SELECT month_start, crime_against,
               SUM(total_incidents) AS total_incidents
        FROM '{_CRIME_MONTHLY}' {w}
        GROUP BY month_start, crime_against
        ORDER BY month_start
    """)


# ── Crime types ──────────────────────────────────────────────────────

def get_crime_types(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, crime_against=crime_against, has_agency=False)
    return _run(f"""
        SELECT offense_group, offense_description, crime_against,
               SUM(count) AS count
        FROM '{_CRIME_BY_TYPE}' {w}
        GROUP BY offense_group, offense_description, crime_against
        ORDER BY count DESC
    """)


# ── Geography / ZIP ──────────────────────────────────────────────────

def get_geography(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, crime_against=crime_against, has_agency=False)
    return _run(f"""
        SELECT zip_code, city, crime_against,
               SUM(count) AS count
        FROM '{_CRIME_BY_ZIP}' {w}
        GROUP BY zip_code, city, crime_against
        ORDER BY count DESC
    """)


# ── Agencies ─────────────────────────────────────────────────────────

def get_agencies(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, crime_against=crime_against)
    return _run(f"""
        SELECT agency_short, crime_against,
               SUM(count) AS count, SUM(dv_count) AS dv_count
        FROM '{_CRIME_BY_AGENCY}' {w}
        GROUP BY agency_short, crime_against
        ORDER BY count DESC
    """)


# ── Victims ──────────────────────────────────────────────────────────

def get_victims(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, crime_against=crime_against, has_agency=False)
    return _run(f"""
        SELECT age_bin, victim_race, victim_sex, crime_against,
               SUM(count) AS count
        FROM '{_VICTIM_DEMO}' {w}
        GROUP BY age_bin, victim_race, victim_sex, crime_against
        ORDER BY count DESC
    """)


# ── Domestic Violence ────────────────────────────────────────────────

def get_domestic_violence(
    year_min: int = 2021, year_max: int = 2025,
    agency: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, has_crime_against=False)
    if agency:
        w = _q(w, f"agency = '{agency.replace(chr(39), chr(39)*2)}'")
    return _run(f"""
        SELECT agency, offense_group, victim_sex, month_start,
               SUM(count) AS count
        FROM '{_DV}' {w}
        GROUP BY agency, offense_group, victim_sex, month_start
        ORDER BY month_start
    """)


# ── Temporal patterns ────────────────────────────────────────────────

def get_temporal_patterns(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, crime_against=crime_against, has_agency=False)
    return _run(f"""
        SELECT dow, month, crime_against,
               SUM(count) AS count
        FROM '{_TEMPORAL}' {w}
        GROUP BY dow, month, crime_against
        ORDER BY dow, month
    """)


# ── Cities ───────────────────────────────────────────────────────────

def get_cities(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, crime_against=crime_against, has_agency=False, has_city=False)
    return _run(f"""
        SELECT city, crime_against,
               SUM(count) AS count
        FROM '{_CRIME_BY_CITY}' {w}
        GROUP BY city, crime_against
        ORDER BY count DESC
    """)


# ── Arrests (Group B) ───────────────────────────────────────────────

def get_arrests(
    year_min: int = 2021, year_max: int = 2025,
    agency: str | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, agency, has_crime_against=False)
    return _run(f"""
        SELECT offense_description, agency_short, month_start,
               SUM(count) AS count
        FROM '{_ARRESTS}' {w}
        GROUP BY offense_description, agency_short, month_start
        ORDER BY month_start
    """)


# ── CFS monthly ─────────────────────────────────────────────────────

def get_calls_for_service(
    year_min: int = 2015, year_max: int = 2026,
    priority: int | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, has_agency=False, has_crime_against=False)
    if priority is not None:
        w = _q(w, f"priority = {priority}")
    return _run(f"""
        SELECT month_start, priority,
               SUM(total_calls) AS total_calls
        FROM '{_CFS_MONTHLY}' {w}
        GROUP BY month_start, priority
        ORDER BY month_start
    """)


# ── CFS by beat ──────────────────────────────────────────────────────

def get_calls_by_beat(
    year_min: int = 2015, year_max: int = 2026,
    priority: int | None = None,
) -> list[dict]:
    w = _where(year_min, year_max, has_agency=False, has_crime_against=False)
    if priority is not None:
        w = _q(w, f"priority = {priority}")
    return _run(f"""
        SELECT beat, priority,
               SUM(total_calls) AS total_calls
        FROM '{_CFS_BY_BEAT}' {w}
        GROUP BY beat, priority
        ORDER BY total_calls DESC
        LIMIT 100
    """)


# ── CFS temporal ─────────────────────────────────────────────────────

def get_calls_temporal(
    priority: int | None = None,
) -> list[dict]:
    w = ""
    if priority is not None:
        w = f"WHERE priority = {priority}"
    return _run(f"""
        SELECT dow, hour, priority,
               SUM(total_calls) AS total_calls
        FROM '{_CFS_TEMPORAL}' {w}
        GROUP BY dow, hour, priority
        ORDER BY dow, hour
    """)
