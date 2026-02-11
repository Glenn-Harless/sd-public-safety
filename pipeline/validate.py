"""Validate processed crime, arrest, and CFS data."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
AGGREGATED_DIR = Path(__file__).resolve().parent.parent / "data" / "aggregated"

SD_LAT_MIN, SD_LAT_MAX = 32.5, 33.3
SD_LNG_MIN, SD_LNG_MAX = -117.7, -116.8

CURRENT_YEAR = datetime.now().year


def _q(sql: str) -> list:
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _scalar(sql: str):
    rows = _q(sql)
    return rows[0][0] if rows else None


def _header(num: int, title: str) -> None:
    print(f"\n{'─' * 64}")
    print(f"  Check {num}: {title}")
    print(f"{'─' * 64}")


def validate() -> int:
    """Run all validation checks. Returns count of issues found."""
    issues = 0
    crime = PROCESSED_DIR / "crime.parquet"
    arrests = PROCESSED_DIR / "arrests.parquet"
    cfs = PROCESSED_DIR / "cfs.parquet"

    # ── Check 1: File existence ──
    _header(1, "File existence")
    for name, path in [("crime", crime), ("arrests", arrests), ("cfs", cfs)]:
        if path.exists():
            size_mb = path.stat().st_size / (1 << 20)
            print(f"  PASS  {name}: {size_mb:.1f} MB")
        else:
            print(f"  FAIL  {name}: NOT FOUND")
            issues += 1

    # ── Check 2: Crime row count ──
    _header(2, "Crime row count (expect >500K)")
    if crime.exists():
        count = _scalar(f"SELECT COUNT(*) FROM '{crime}'")
        if count and count > 500_000:
            print(f"  PASS  {count:,} rows")
        else:
            print(f"  WARN  {count:,} rows (expected >500K)")
            issues += 1

    # ── Check 3: Crime date range ──
    _header(3, "Crime date range (expect 2021-current)")
    if crime.exists():
        min_yr = _scalar(f"SELECT MIN(year) FROM '{crime}'")
        max_yr = _scalar(f"SELECT MAX(year) FROM '{crime}'")
        if min_yr and min_yr <= 2021 and max_yr and max_yr >= CURRENT_YEAR - 1:
            print(f"  PASS  {min_yr} - {max_yr}")
        else:
            print(f"  WARN  {min_yr} - {max_yr}")
            issues += 1

    # ── Check 4: Geographic bounds ──
    _header(4, "Geographic bounds (San Diego county)")
    if crime.exists():
        outliers = _scalar(f"""
            SELECT COUNT(*) FROM '{crime}'
            WHERE lat IS NOT NULL AND (
                lat < {SD_LAT_MIN} OR lat > {SD_LAT_MAX}
                OR lng < {SD_LNG_MIN} OR lng > {SD_LNG_MAX}
            )
        """)
        total_geo = _scalar(f"SELECT COUNT(*) FROM '{crime}' WHERE lat IS NOT NULL")
        pct = (outliers / total_geo * 100) if total_geo else 0
        if pct < 1:
            print(f"  PASS  {outliers:,} outliers ({pct:.2f}% of geo records)")
        else:
            print(f"  WARN  {outliers:,} outliers ({pct:.2f}%)")
            issues += 1

    # ── Check 5: NULL rates on critical columns ──
    _header(5, "NULL rates on critical crime columns (<5%)")
    if crime.exists():
        total = _scalar(f"SELECT COUNT(*) FROM '{crime}'")
        for col in ["incident_date", "agency_short", "crime_against", "offense_group"]:
            nulls = _scalar(f"SELECT COUNT(*) FROM '{crime}' WHERE {col} IS NULL")
            pct = (nulls / total * 100) if total else 0
            status = "PASS" if pct < 5 else "WARN"
            if pct >= 5:
                issues += 1
            print(f"  {status}  {col}: {nulls:,} nulls ({pct:.1f}%)")

    # ── Check 6: Agency distribution (SDPD ~45%) ──
    _header(6, "Agency distribution")
    if crime.exists():
        rows = _q(f"""
            SELECT agency_short, COUNT(*) AS n,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
            FROM '{crime}'
            GROUP BY agency_short
            ORDER BY n DESC
            LIMIT 5
        """)
        for agency, n, pct in rows:
            print(f"  INFO  {agency}: {n:,} ({pct}%)")

    # ── Check 7: Crime category presence ──
    _header(7, "Crime categories present")
    if crime.exists():
        cats = _q(f"SELECT DISTINCT crime_against FROM '{crime}' WHERE crime_against IS NOT NULL ORDER BY 1")
        cats_list = [r[0] for r in cats]
        expected = {"People", "Property", "Society"}
        if expected.issubset(set(cats_list)):
            print(f"  PASS  {cats_list}")
        else:
            print(f"  WARN  {cats_list} (expected {expected})")
            issues += 1

    # ── Check 8: Year-over-year anomalies (>50% change) ──
    _header(8, "Year-over-year crime volume anomalies")
    if crime.exists():
        rows = _q(f"""
            SELECT year, COUNT(*) AS n FROM '{crime}'
            WHERE year IS NOT NULL
            GROUP BY year ORDER BY year
        """)
        for i in range(1, len(rows)):
            prev_yr, prev_n = rows[i - 1]
            curr_yr, curr_n = rows[i]
            change = (curr_n - prev_n) / prev_n * 100 if prev_n else 0
            status = "WARN" if abs(change) > 50 else "PASS"
            if abs(change) > 50:
                issues += 1
            print(f"  {status}  {prev_yr}->{curr_yr}: {change:+.1f}% ({prev_n:,} -> {curr_n:,})")

    # ── Check 9: Crime dedup check ──
    _header(9, "Crime deduplication")
    if crime.exists():
        dupes = _scalar(f"""
            SELECT COUNT(*) FROM (
                SELECT incidentuid, offense_description
                FROM '{crime}'
                GROUP BY incidentuid, offense_description
                HAVING COUNT(*) > 1
            )
        """)
        if dupes == 0:
            print(f"  PASS  No duplicates on (incidentuid, offense_description)")
        else:
            print(f"  FAIL  {dupes:,} duplicate groups")
            issues += 1

    # ── Check 10: Arrests validation ──
    _header(10, "Arrests row count and offense types")
    if arrests.exists():
        count = _scalar(f"SELECT COUNT(*) FROM '{arrests}'")
        status = "PASS" if count and count > 40_000 else "WARN"
        if count and count <= 40_000:
            issues += 1
        print(f"  {status}  {count:,} arrest records")

        types = _q(f"SELECT DISTINCT offense_description FROM '{arrests}' ORDER BY 1")
        types_list = [r[0] for r in types]
        print(f"  INFO  Offense types: {types_list}")

    # ── Check 11: Arrests date range ──
    _header(11, "Arrests date range")
    if arrests.exists():
        min_yr = _scalar(f"SELECT MIN(year) FROM '{arrests}'")
        max_yr = _scalar(f"SELECT MAX(year) FROM '{arrests}'")
        print(f"  INFO  {min_yr} - {max_yr}")

    # ── Check 12: CFS row count ──
    _header(12, "CFS row count (expect >3M)")
    if cfs.exists():
        count = _scalar(f"SELECT COUNT(*) FROM '{cfs}'")
        if count and count > 3_000_000:
            print(f"  PASS  {count:,} calls")
        else:
            print(f"  WARN  {count:,} calls (expected >3M)")
            issues += 1

    # ── Check 13: CFS date range and year coverage ──
    _header(13, "CFS date range (expect 2015-current)")
    if cfs.exists():
        min_yr = _scalar(f"SELECT MIN(year) FROM '{cfs}'")
        max_yr = _scalar(f"SELECT MAX(year) FROM '{cfs}'")
        years = _q(f"SELECT DISTINCT year FROM '{cfs}' WHERE year IS NOT NULL ORDER BY 1")
        years_list = [r[0] for r in years]
        if min_yr and min_yr <= 2015 and max_yr and max_yr >= CURRENT_YEAR - 1:
            print(f"  PASS  {min_yr} - {max_yr}")
        else:
            print(f"  WARN  {min_yr} - {max_yr}")
            issues += 1
        print(f"  INFO  Years present: {years_list}")

    # ── Check 14: CFS beat coverage ──
    _header(14, "CFS beat coverage")
    if cfs.exists():
        beat_count = _scalar(f"SELECT COUNT(DISTINCT beat) FROM '{cfs}' WHERE beat IS NOT NULL")
        print(f"  INFO  {beat_count} distinct beats")

    # ── Check 15: Aggregation files ──
    _header(15, "Aggregation file existence and sizes")
    expected_aggs = [
        "crime_overview_monthly", "crime_by_type", "crime_by_zip",
        "crime_by_agency", "victim_demographics", "domestic_violence",
        "temporal_patterns", "map_points", "yearly_summary", "crime_by_city",
        "arrests_by_type",
        "cfs_monthly", "cfs_by_beat", "cfs_temporal",
    ]
    for name in expected_aggs:
        path = AGGREGATED_DIR / f"{name}.parquet"
        if path.exists():
            count = _scalar(f"SELECT COUNT(*) FROM '{path}'")
            size_mb = path.stat().st_size / (1 << 20)
            print(f"  PASS  {name}: {count:,} rows ({size_mb:.1f} MB)")
        else:
            print(f"  FAIL  {name}: NOT FOUND")
            issues += 1

    # ── Summary ──
    print(f"\n{'=' * 64}")
    if issues == 0:
        print("  All checks passed!")
    else:
        print(f"  {issues} issue(s) found")
    print(f"{'=' * 64}")

    return issues


if __name__ == "__main__":
    validate()
