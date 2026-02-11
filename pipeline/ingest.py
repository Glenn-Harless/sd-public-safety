"""Download CIBRS crime data (SANDAG SODA API) and Calls for Service CSVs."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import httpx

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
CURRENT_YEAR = datetime.now().year

# ── SODA API sources (SANDAG) ──────────────────────────────────────────
SODA_BASE = "https://opendata.sandag.org/resource"
GROUP_A_ID = "7sps-5pd9"
GROUP_B_ID = "huzf-mi2z"
SODA_PAGE_SIZE = 50_000

# Group B: only these offense codes (exclude "All Other Offenses" catch-all)
GROUP_B_CODES = ("90D", "90C", "90B", "90E")  # DUI, disorderly, trespass, vagrancy

# ── CFS CSV sources (SDPD) ────────────────────────────────────────────
CFS_BASE = "https://seshat.datasd.org/police_calls_for_service"
CFS_YEARS = range(2015, CURRENT_YEAR + 1)
CFS_REF_FILES = {
    "pd_cfs_calltypes_datasd.csv": "call_type_desc.csv",
    "pd_dispo_codes_datasd.csv": "dispo_code_desc.csv",
}
CFS_DISPO_BASE = "https://seshat.datasd.org/pd"


def _soda_fetch(
    dataset_id: str,
    out_path: Path,
    where: str | None = None,
    *,
    force: bool = False,
) -> Path:
    """Paginate a SODA API dataset and save as JSON."""
    if out_path.exists() and not force:
        print(f"  cached: {out_path.name}")
        return out_path

    url = f"{SODA_BASE}/{dataset_id}.json"
    all_rows: list[dict] = []
    offset = 0

    with httpx.Client(timeout=300, follow_redirects=True) as client:
        while True:
            params: dict[str, str | int] = {
                "$limit": SODA_PAGE_SIZE,
                "$offset": offset,
                "$order": ":id",
            }
            if where:
                params["$where"] = where

            print(f"  fetching offset={offset} ...", end=" ", flush=True)
            resp = client.get(url, params=params)
            resp.raise_for_status()
            batch = resp.json()
            print(f"{len(batch)} rows")

            if not batch:
                break
            all_rows.extend(batch)
            if len(batch) < SODA_PAGE_SIZE:
                break
            offset += SODA_PAGE_SIZE

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(all_rows))
    print(f"  saved {len(all_rows)} rows -> {out_path.name}")
    return out_path


def _csv_download(url: str, out_path: Path, *, force: bool = False) -> Path | None:
    """Stream-download a CSV file. Returns None on 403 (future year)."""
    if out_path.exists() and not force:
        print(f"  cached: {out_path.name}")
        return out_path

    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=1 << 20):
                    f.write(chunk)
        size_mb = out_path.stat().st_size / (1 << 20)
        print(f"  downloaded: {out_path.name} ({size_mb:.1f} MB)")
        return out_path
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            print(f"  skipped (403): {out_path.name}")
            return None
        raise


def ingest(force: bool = False) -> list[Path]:
    """Download all data sources. Returns list of downloaded file paths."""
    paths: list[Path] = []

    # ── Group A: CIBRS incidents ──
    print("\n  CIBRS Group A (incidents):")
    p = _soda_fetch(
        GROUP_A_ID,
        RAW_DIR / "cibrs_group_a.json",
        force=force,
    )
    paths.append(p)

    # ── Group B: CIBRS arrests (filtered) ──
    print("\n  CIBRS Group B (arrests):")
    codes = " OR ".join(f"offense_code='{c}'" for c in GROUP_B_CODES)
    p = _soda_fetch(
        GROUP_B_ID,
        RAW_DIR / "cibrs_group_b.json",
        where=codes,
        force=force,
    )
    paths.append(p)

    # ── Calls for Service CSVs ──
    print("\n  Calls for Service:")
    for year in CFS_YEARS:
        url = f"{CFS_BASE}/pd_calls_for_service_{year}_datasd.csv"
        out = RAW_DIR / f"cfs_{year}.csv"
        p = _csv_download(url, out, force=force)
        if p:
            paths.append(p)

    # ── CFS reference files ──
    print("\n  CFS reference files:")
    for remote_name, local_name in CFS_REF_FILES.items():
        if remote_name.startswith("pd_dispo"):
            url = f"{CFS_DISPO_BASE}/{remote_name}"
        else:
            url = f"{CFS_BASE}/{remote_name}"
        out = RAW_DIR / local_name
        p = _csv_download(url, out, force=force)
        if p:
            paths.append(p)

    return paths


if __name__ == "__main__":
    import sys

    ingest(force="--force" in sys.argv)
