"""Transform raw crime, arrest, and CFS data into Parquet files + aggregations."""

from __future__ import annotations

from pathlib import Path

import duckdb

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
AGGREGATED_DIR = Path(__file__).resolve().parent.parent / "data" / "aggregated"

_SD_LAT_MIN, _SD_LAT_MAX = 32.5, 33.3
_SD_LNG_MIN, _SD_LNG_MAX = -117.7, -116.8

# Agency short-name mapping (agency field is city name like "SAN DIEGO")
_AGENCY_CASE = """
    CASE
        WHEN agency ILIKE 'SAN DIEGO' THEN 'SDPD'
        WHEN agency ILIKE '%SHERIFF%' OR agency ILIKE '%SD COUNTY%' THEN 'SDSO'
        WHEN agency ILIKE 'CHULA VISTA' THEN 'CVPD'
        WHEN agency ILIKE 'OCEANSIDE' THEN 'OPD'
        WHEN agency ILIKE 'ESCONDIDO' THEN 'EPD'
        WHEN agency ILIKE 'CARLSBAD' THEN 'CPD'
        WHEN agency ILIKE 'EL CAJON' THEN 'ECPD'
        WHEN agency ILIKE 'NATIONAL CITY' THEN 'NCPD'
        WHEN agency ILIKE 'LA MESA' THEN 'LMPD'
        WHEN agency ILIKE 'CORONADO' THEN 'CoronPD'
        WHEN agency ILIKE 'VISTA' THEN 'VPD'
        ELSE UPPER(REPLACE(agency, ' ', ''))
    END
"""


def _export(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> int:
    """Run COPY ... TO parquet ZSTD. Returns row count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    count = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    size_mb = path.stat().st_size / (1 << 20)
    print(f"    {path.name}: {count:,} rows ({size_mb:.1f} MB)")
    return count


# ── Track 1: CIBRS Group A -> crime.parquet ──────────────────────────────

def _transform_crime(con: duckdb.DuckDBPyConnection) -> None:
    """Process CIBRS Group A incidents."""
    src = RAW_DIR / "cibrs_group_a.json"
    dest = PROCESSED_DIR / "crime.parquet"
    print("\n  Track 1: CIBRS Group A -> crime.parquet")

    con.execute(f"""
        CREATE OR REPLACE TABLE raw_crime AS
        SELECT * FROM read_json('{src}', auto_detect=true, format='array')
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE crime AS
        SELECT * EXCLUDE (_rn) FROM (
            SELECT
                incidentuid,
                TRY_CAST(incident_date AS DATE) AS incident_date,
                YEAR(TRY_CAST(incident_date AS DATE)) AS year,
                MONTH(TRY_CAST(incident_date AS DATE)) AS month,
                QUARTER(TRY_CAST(incident_date AS DATE)) AS quarter,
                DAYOFWEEK(TRY_CAST(incident_date AS DATE)) AS dow,
                DATE_TRUNC('month', TRY_CAST(incident_date AS DATE)) AS month_start,
                agency,
                {_AGENCY_CASE} AS agency_short,
                crime_against_category AS crime_against,
                cibrs_grouped_offense_description AS offense_group,
                cibrs_offense_description AS offense_description,
                TRY_CAST(victim_age AS INT) AS victim_age,
                victim_race,
                victim_sex,
                zip_code,
                city,
                COALESCE(domestic_violence_incident, false) AS is_domestic_violence,
                CASE WHEN TRY_CAST(stolen_vehicles AS INT) > 0 THEN true
                     WHEN LOWER(cibrs_offense_description) LIKE '%motor vehicle theft%' THEN true
                     ELSE false END AS is_stolen_vehicle,
                TRY_CAST(location.coordinates[2] AS DOUBLE) AS lat,
                TRY_CAST(location.coordinates[1] AS DOUBLE) AS lng,
                ROW_NUMBER() OVER (
                    PARTITION BY incidentuid, cibrs_offense_description
                    ORDER BY TRY_CAST(incident_date AS TIMESTAMP) DESC NULLS LAST
                ) AS _rn
            FROM raw_crime
        )
        WHERE _rn = 1
    """)

    count = _export(con, "SELECT * FROM crime", dest)
    print(f"    -> {count:,} deduplicated incidents")


# ── Track 2: CIBRS Group B -> arrests.parquet ───────────────────────────

def _transform_arrests(con: duckdb.DuckDBPyConnection) -> None:
    """Process CIBRS Group B arrests."""
    src = RAW_DIR / "cibrs_group_b.json"
    dest = PROCESSED_DIR / "arrests.parquet"
    print("\n  Track 2: CIBRS Group B -> arrests.parquet")

    con.execute(f"""
        CREATE OR REPLACE TABLE raw_arrests AS
        SELECT * FROM read_json('{src}', auto_detect=true, format='array')
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE arrests AS
        SELECT
            incident_uid AS incidentuid,
            TRY_CAST(arrest_date AS DATE) AS incident_date,
            YEAR(TRY_CAST(arrest_date AS DATE)) AS year,
            MONTH(TRY_CAST(arrest_date AS DATE)) AS month,
            QUARTER(TRY_CAST(arrest_date AS DATE)) AS quarter,
            DAYOFWEEK(TRY_CAST(arrest_date AS DATE)) AS dow,
            DATE_TRUNC('month', TRY_CAST(arrest_date AS DATE)) AS month_start,
            arrest_agency AS agency,
            CASE
                WHEN arrest_agency ILIKE 'SAN DIEGO' THEN 'SDPD'
                WHEN arrest_agency ILIKE '%SHERIFF%' OR arrest_agency ILIKE '%SD COUNTY%' THEN 'SDSO'
                WHEN arrest_agency ILIKE 'CHULA VISTA' THEN 'CVPD'
                WHEN arrest_agency ILIKE 'OCEANSIDE' THEN 'OPD'
                WHEN arrest_agency ILIKE 'ESCONDIDO' THEN 'EPD'
                WHEN arrest_agency ILIKE 'CARLSBAD' THEN 'CPD'
                WHEN arrest_agency ILIKE 'EL CAJON' THEN 'ECPD'
                WHEN arrest_agency ILIKE 'NATIONAL CITY' THEN 'NCPD'
                WHEN arrest_agency ILIKE 'LA MESA' THEN 'LMPD'
                WHEN arrest_agency ILIKE 'CORONADO' THEN 'CoronPD'
                WHEN arrest_agency ILIKE 'VISTA' THEN 'VPD'
                ELSE UPPER(REPLACE(arrest_agency, ' ', ''))
            END AS agency_short,
            offense_code,
            offense_description
        FROM raw_arrests
    """)

    count = _export(con, "SELECT * FROM arrests", dest)
    print(f"    -> {count:,} arrest records")


# ── Track 3: CFS -> cfs.parquet ─────────────────────────────────────────

def _load_cfs_reference_tables(con: duckdb.DuckDBPyConnection) -> tuple[bool, bool]:
    """Load CFS reference CSVs if they exist. Returns (has_call_type, has_dispo)."""
    call_type_csv = RAW_DIR / "call_type_desc.csv"
    dispo_csv = RAW_DIR / "dispo_code_desc.csv"

    has_call_type = False
    has_dispo = False

    if call_type_csv.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE ref_call_type AS
            SELECT * FROM read_csv('{call_type_csv}', header=true, auto_detect=true)
        """)
        has_call_type = True
        count = con.execute("SELECT COUNT(*) FROM ref_call_type").fetchone()[0]
        print(f"    loaded ref_call_type: {count} rows")

    if dispo_csv.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE ref_dispo AS
            SELECT * FROM read_csv('{dispo_csv}', header=true, auto_detect=true)
        """)
        has_dispo = True
        count = con.execute("SELECT COUNT(*) FROM ref_dispo").fetchone()[0]
        print(f"    loaded ref_dispo: {count} rows")

    return has_call_type, has_dispo


def _transform_cfs(con: duckdb.DuckDBPyConnection) -> None:
    """Process Calls for Service CSVs."""
    dest = PROCESSED_DIR / "cfs.parquet"
    print("\n  Track 3: CFS -> cfs.parquet")

    csv_files = sorted(RAW_DIR.glob("cfs_*.csv"))
    if not csv_files:
        print("    WARNING: no CFS CSV files found, skipping")
        return

    csv_list = ", ".join(f"'{f}'" for f in csv_files)

    con.execute(f"""
        CREATE OR REPLACE TABLE raw_cfs AS
        SELECT * FROM read_csv(
            [{csv_list}],
            header = true,
            ignore_errors = true,
            union_by_name = true,
            filename = true
        )
    """)

    # Load reference tables for human-readable descriptions
    has_call_type, has_dispo = _load_cfs_reference_tables(con)

    # Build the call_type_desc and dispo_desc expressions based on
    # whether reference tables are available.
    # Reference CSVs use columns: CALL_TYPE / DESCRIPTION for call types,
    # and DISPO_CODE / DESCRIPTION for dispositions.
    if has_call_type:
        call_type_expr = "COALESCE(ct.DESCRIPTION, cfs_raw.CALL_TYPE)"
        call_type_join = "LEFT JOIN ref_call_type ct ON cfs_raw.CALL_TYPE = ct.CALL_TYPE"
    else:
        call_type_expr = "cfs_raw.CALL_TYPE"
        call_type_join = ""

    if has_dispo:
        dispo_expr = "COALESCE(dd.DESCRIPTION, cfs_raw.DISPOSITION)"
        dispo_join = "LEFT JOIN ref_dispo dd ON cfs_raw.DISPOSITION = dd.DISPO_CODE"
    else:
        dispo_expr = "cfs_raw.DISPOSITION"
        dispo_join = ""

    # CFS columns are uppercase: INCIDENT_NUM, DATE_TIME, CALL_TYPE, etc.
    con.execute(f"""
        CREATE OR REPLACE TABLE cfs_dedup AS
        SELECT * EXCLUDE (_rn) FROM (
            SELECT
                cfs_raw.INCIDENT_NUM AS incident_num,
                TRY_CAST(cfs_raw.DATE_TIME AS TIMESTAMP) AS call_timestamp,
                TRY_CAST(cfs_raw.DATE_TIME AS DATE) AS call_date,
                YEAR(TRY_CAST(cfs_raw.DATE_TIME AS DATE)) AS year,
                MONTH(TRY_CAST(cfs_raw.DATE_TIME AS DATE)) AS month,
                DAYOFWEEK(TRY_CAST(cfs_raw.DATE_TIME AS DATE)) AS dow,
                HOUR(TRY_CAST(cfs_raw.DATE_TIME AS TIMESTAMP)) AS hour,
                DATE_TRUNC('month', TRY_CAST(cfs_raw.DATE_TIME AS DATE)) AS month_start,
                cfs_raw.CALL_TYPE AS call_type,
                {call_type_expr} AS call_type_desc,
                TRY_CAST(cfs_raw.PRIORITY AS INT) AS priority,
                cfs_raw.DISPOSITION AS disposition,
                {dispo_expr} AS dispo_desc,
                TRY_CAST(cfs_raw.BEAT AS VARCHAR) AS beat,
                ROW_NUMBER() OVER (
                    PARTITION BY cfs_raw.INCIDENT_NUM
                    ORDER BY TRY_CAST(cfs_raw.DATE_TIME AS TIMESTAMP) DESC NULLS LAST
                ) AS _rn
            FROM raw_cfs cfs_raw
            {call_type_join}
            {dispo_join}
        )
        WHERE _rn = 1
    """)

    count = _export(con, "SELECT * FROM cfs_dedup", dest)
    print(f"    -> {count:,} deduplicated calls")


# ── Aggregations ────────────────────────────────────────────────────────

def _build_aggregations(con: duckdb.DuckDBPyConnection) -> None:
    """Build all 14 aggregation parquet files."""
    print("\n  Building aggregations:")

    crime_path = PROCESSED_DIR / "crime.parquet"
    arrests_path = PROCESSED_DIR / "arrests.parquet"
    cfs_path = PROCESSED_DIR / "cfs.parquet"

    # ── Crime aggregations (Group A) ──
    if crime_path.exists():
        C = f"'{crime_path}'"

        _export(con, f"""
            SELECT month_start, agency_short, crime_against,
                   COUNT(*) AS total_incidents
            FROM {C}
            WHERE month_start IS NOT NULL
            GROUP BY month_start, agency_short, crime_against
            ORDER BY month_start
        """, AGGREGATED_DIR / "crime_overview_monthly.parquet")

        _export(con, f"""
            SELECT offense_group, offense_description, crime_against, year,
                   COUNT(*) AS count
            FROM {C}
            WHERE year IS NOT NULL
            GROUP BY offense_group, offense_description, crime_against, year
            ORDER BY count DESC
        """, AGGREGATED_DIR / "crime_by_type.parquet")

        _export(con, f"""
            SELECT zip_code, city, year, crime_against,
                   COUNT(*) AS count
            FROM {C}
            WHERE zip_code IS NOT NULL AND year IS NOT NULL
            GROUP BY zip_code, city, year, crime_against
            ORDER BY count DESC
        """, AGGREGATED_DIR / "crime_by_zip.parquet")

        _export(con, f"""
            SELECT agency_short, year, crime_against,
                   COUNT(*) AS count,
                   SUM(CASE WHEN is_domestic_violence THEN 1 ELSE 0 END) AS dv_count
            FROM {C}
            WHERE year IS NOT NULL
            GROUP BY agency_short, year, crime_against
            ORDER BY count DESC
        """, AGGREGATED_DIR / "crime_by_agency.parquet")

        _export(con, f"""
            SELECT
                CASE
                    WHEN victim_age < 18 THEN 'Under 18'
                    WHEN victim_age BETWEEN 18 AND 24 THEN '18-24'
                    WHEN victim_age BETWEEN 25 AND 34 THEN '25-34'
                    WHEN victim_age BETWEEN 35 AND 44 THEN '35-44'
                    WHEN victim_age BETWEEN 45 AND 54 THEN '45-54'
                    WHEN victim_age BETWEEN 55 AND 64 THEN '55-64'
                    WHEN victim_age >= 65 THEN '65+'
                    ELSE 'Unknown'
                END AS age_bin,
                victim_race, victim_sex, crime_against, year,
                COUNT(*) AS count
            FROM {C}
            WHERE year IS NOT NULL
            GROUP BY age_bin, victim_race, victim_sex, crime_against, year
            ORDER BY count DESC
        """, AGGREGATED_DIR / "victim_demographics.parquet")

        _export(con, f"""
            SELECT agency_short AS agency, offense_group, victim_sex,
                   year, month_start,
                   COUNT(*) AS count
            FROM {C}
            WHERE is_domestic_violence = true AND year IS NOT NULL
            GROUP BY agency_short, offense_group, victim_sex, year, month_start
            ORDER BY month_start
        """, AGGREGATED_DIR / "domestic_violence.parquet")

        _export(con, f"""
            SELECT dow, month, year, crime_against,
                   COUNT(*) AS count
            FROM {C}
            WHERE year IS NOT NULL
            GROUP BY dow, month, year, crime_against
        """, AGGREGATED_DIR / "temporal_patterns.parquet")

        _export(con, f"""
            SELECT lat, lng, offense_group, crime_against,
                   agency_short AS agency, year, city
            FROM {C}
            WHERE lat IS NOT NULL AND lng IS NOT NULL
              AND lat BETWEEN {_SD_LAT_MIN} AND {_SD_LAT_MAX}
              AND lng BETWEEN {_SD_LNG_MIN} AND {_SD_LNG_MAX}
        """, AGGREGATED_DIR / "map_points.parquet")

        _export(con, f"""
            SELECT year,
                   COUNT(*) AS total,
                   SUM(CASE WHEN crime_against = 'People' THEN 1 ELSE 0 END) AS person_crimes,
                   SUM(CASE WHEN crime_against = 'Property' THEN 1 ELSE 0 END) AS property_crimes,
                   SUM(CASE WHEN crime_against = 'Society' THEN 1 ELSE 0 END) AS society_crimes,
                   SUM(CASE WHEN is_domestic_violence THEN 1 ELSE 0 END) AS dv_total,
                   SUM(CASE WHEN is_stolen_vehicle THEN 1 ELSE 0 END) AS stolen_vehicle_total
            FROM {C}
            WHERE year IS NOT NULL
            GROUP BY year
            ORDER BY year
        """, AGGREGATED_DIR / "yearly_summary.parquet")

        _export(con, f"""
            SELECT city, year, crime_against,
                   COUNT(*) AS count
            FROM {C}
            WHERE city IS NOT NULL AND year IS NOT NULL
            GROUP BY city, year, crime_against
            ORDER BY count DESC
        """, AGGREGATED_DIR / "crime_by_city.parquet")

    # ── Arrest aggregations (Group B) ──
    if arrests_path.exists():
        A = f"'{arrests_path}'"

        _export(con, f"""
            SELECT offense_description, agency_short, year, month_start,
                   COUNT(*) AS count
            FROM {A}
            WHERE year IS NOT NULL
            GROUP BY offense_description, agency_short, year, month_start
            ORDER BY month_start
        """, AGGREGATED_DIR / "arrests_by_type.parquet")

    # ── CFS aggregations ──
    if cfs_path.exists():
        F = f"'{cfs_path}'"

        _export(con, f"""
            SELECT month_start, year, call_type_desc, priority,
                   COUNT(*) AS total_calls
            FROM {F}
            WHERE month_start IS NOT NULL
            GROUP BY month_start, year, call_type_desc, priority
            ORDER BY month_start
        """, AGGREGATED_DIR / "cfs_monthly.parquet")

        _export(con, f"""
            SELECT beat, year, call_type_desc, priority, disposition,
                   COUNT(*) AS total_calls
            FROM {F}
            WHERE beat IS NOT NULL AND year IS NOT NULL
            GROUP BY beat, year, call_type_desc, priority, disposition
            ORDER BY total_calls DESC
        """, AGGREGATED_DIR / "cfs_by_beat.parquet")

        _export(con, f"""
            SELECT dow, hour, priority,
                   COUNT(*) AS total_calls
            FROM {F}
            WHERE dow IS NOT NULL AND hour IS NOT NULL
            GROUP BY dow, hour, priority
        """, AGGREGATED_DIR / "cfs_temporal.parquet")


def transform() -> None:
    """Run all three transform tracks + aggregations."""
    con = duckdb.connect()
    try:
        _transform_crime(con)
        _transform_arrests(con)
        _transform_cfs(con)
        _build_aggregations(con)
    finally:
        con.close()


if __name__ == "__main__":
    transform()
