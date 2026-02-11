"""FastAPI application for San Diego Public Safety data."""

from __future__ import annotations

from fastapi import FastAPI, Query

from api import queries
from api.models import (
    AgencyRow,
    ArrestRow,
    CFSBeatRow,
    CFSRow,
    CFSTemporalRow,
    CityRow,
    CrimeType,
    DVRow,
    FilterOptions,
    GeographyRow,
    TemporalRow,
    TrendPoint,
    VictimRow,
    YearlySummary,
)

app = FastAPI(
    title="San Diego Public Safety API",
    description="Crime incidents, arrests, and calls for service across San Diego County",
    version="0.1.0",
)


@app.get("/")
def root():
    return {
        "message": "San Diego Public Safety API",
        "endpoints": [
            "/filters", "/overview", "/trends", "/crime-types",
            "/geography", "/agencies", "/victims", "/domestic-violence",
            "/temporal-patterns", "/cities", "/arrests",
            "/calls-for-service", "/calls-by-beat", "/calls-temporal",
        ],
    }


@app.get("/filters", response_model=FilterOptions)
def filters():
    """Available filter values for all datasets."""
    return queries.get_filter_options()


@app.get("/overview", response_model=list[YearlySummary])
def overview(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    agency: str | None = Query(None, description="Agency short name"),
):
    return queries.get_overview(year_min, year_max, agency)


@app.get("/trends", response_model=list[TrendPoint])
def trends(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    agency: str | None = Query(None, description="Agency short name"),
    crime_against: str | None = Query(None, description="Crime category"),
):
    return queries.get_trends(year_min, year_max, agency, crime_against)


@app.get("/crime-types", response_model=list[CrimeType])
def crime_types(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    crime_against: str | None = Query(None, description="Crime category"),
):
    return queries.get_crime_types(year_min, year_max, crime_against)


@app.get("/geography", response_model=list[GeographyRow])
def geography(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    crime_against: str | None = Query(None, description="Crime category"),
):
    return queries.get_geography(year_min, year_max, crime_against)


@app.get("/agencies", response_model=list[AgencyRow])
def agencies(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    crime_against: str | None = Query(None, description="Crime category"),
):
    return queries.get_agencies(year_min, year_max, crime_against)


@app.get("/victims", response_model=list[VictimRow])
def victims(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    crime_against: str | None = Query(None, description="Crime category"),
):
    return queries.get_victims(year_min, year_max, crime_against)


@app.get("/domestic-violence", response_model=list[DVRow])
def domestic_violence(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    agency: str | None = Query(None, description="Agency short name"),
):
    return queries.get_domestic_violence(year_min, year_max, agency)


@app.get("/temporal-patterns", response_model=list[TemporalRow])
def temporal_patterns(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    crime_against: str | None = Query(None, description="Crime category"),
):
    return queries.get_temporal_patterns(year_min, year_max, crime_against)


@app.get("/cities", response_model=list[CityRow])
def cities(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    crime_against: str | None = Query(None, description="Crime category"),
):
    return queries.get_cities(year_min, year_max, crime_against)


@app.get("/arrests", response_model=list[ArrestRow])
def arrests(
    year_min: int = Query(2021, description="Start year"),
    year_max: int = Query(2025, description="End year"),
    agency: str | None = Query(None, description="Agency short name"),
):
    return queries.get_arrests(year_min, year_max, agency)


@app.get("/calls-for-service", response_model=list[CFSRow])
def calls_for_service(
    year_min: int = Query(2015, description="Start year"),
    year_max: int = Query(2026, description="End year"),
    priority: int | None = Query(None, description="Call priority level"),
):
    return queries.get_calls_for_service(year_min, year_max, priority)


@app.get("/calls-by-beat", response_model=list[CFSBeatRow])
def calls_by_beat(
    year_min: int = Query(2015, description="Start year"),
    year_max: int = Query(2026, description="End year"),
    priority: int | None = Query(None, description="Call priority level"),
):
    return queries.get_calls_by_beat(year_min, year_max, priority)


@app.get("/calls-temporal", response_model=list[CFSTemporalRow])
def calls_temporal(
    priority: int | None = Query(None, description="Call priority level"),
):
    return queries.get_calls_temporal(priority)
