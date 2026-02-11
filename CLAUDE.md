# San Diego Public Safety & Crime Patterns

## Architecture
Pipeline (ingest/transform/validate) -> Parquet files -> Dashboard (Streamlit) + API (FastAPI) + MCP Server (FastMCP)

## Three Data Sources
1. **CIBRS Group A** (SANDAG SODA API `7sps-5pd9`): 711K+ crime incidents with lat/lng, victim demographics, 11 agencies
2. **CIBRS Group B** (SANDAG SODA API `huzf-mi2z`): ~54K arrests (DUI, disorderly conduct, trespass, vagrancy) — filtered to exclude "All Other Offenses"
3. **Calls for Service** (SDPD CSV): Millions of dispatch records 2015-present

## Key Design Decisions
- DuckDB for all transforms (1GB RAM limit on Streamlit Cloud free tier)
- `query()` helper: fresh `duckdb.connect()` per call, returns pandas DataFrame
- `_where_clause()` uses `has_*` flags because different parquet files have different columns
- Group B kept as separate analytical layer (arrests ≠ incidents)
- CFS joined with reference tables for human-readable descriptions

## File Layout
- `data/raw/` — gitignored, downloaded by pipeline
- `data/processed/` — crime.parquet, arrests.parquet, cfs.parquet (gitignored if >100MB)
- `data/aggregated/` — 14 aggregation parquets (committed)
- `pipeline/` — ingest.py, transform.py, validate.py, build.py
- `api/` — queries.py, models.py, main.py, mcp_server.py
- `dashboard/` — app.py

## Deployment
- Dashboard: Streamlit Cloud via `requirements.txt`
- API: Render via `requirements-api.txt`
- Data refresh: GitHub Actions weekly Monday (`.github/workflows/refresh.yml`)

## Gotchas
- `.gitignore` negation: use `data/processed/*` (not `data/processed/`) + `!data/processed/.gitkeep`
- GeoJSON coordinates are [lng, lat] not [lat, lng]
- Group B has different field names: `incident_uid` -> `incidentuid`, `arrest_agency` -> `agency`, etc.
- CFS CSV columns vary by year — use `union_by_name=true`
- SODA API pagination: `$limit` + `$offset` + `$order=:id`
