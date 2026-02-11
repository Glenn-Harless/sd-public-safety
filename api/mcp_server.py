"""MCP server for San Diego Public Safety data."""

from __future__ import annotations

from fastmcp import FastMCP

from api import queries

mcp = FastMCP(
    "San Diego Public Safety",
    instructions=(
        "San Diego County crime incidents (2021-present), arrests, and police "
        "calls for service (2015-present). Call get_filter_options first to see "
        "available years, agencies, crime categories, and cities. Crime categories "
        "are People, Property, and Society. Agency short names: SDPD, SDSO, CVPD, "
        "OPD, EPD, CPD, ECPD, NCPD, LMPD, CoronPD, VPD."
    ),
)


@mcp.tool()
def get_filter_options() -> dict:
    """Get available filter values: years, agencies, crime categories, cities."""
    return queries.get_filter_options()


@mcp.tool()
def get_overview(year_min: int = 2021, year_max: int = 2025, agency: str | None = None) -> list[dict]:
    """Yearly crime summary with totals by category, DV, and stolen vehicles."""
    return queries.get_overview(year_min, year_max, agency)


@mcp.tool()
def get_trends(
    year_min: int = 2021, year_max: int = 2025,
    agency: str | None = None, crime_against: str | None = None,
) -> list[dict]:
    """Monthly crime trends. Filter by agency and/or crime category."""
    return queries.get_trends(year_min, year_max, agency, crime_against)


@mcp.tool()
def get_crime_types(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    """Crime counts by offense type and category."""
    return queries.get_crime_types(year_min, year_max, crime_against)


@mcp.tool()
def get_geography(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    """Crime counts by ZIP code and city."""
    return queries.get_geography(year_min, year_max, crime_against)


@mcp.tool()
def get_agencies(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    """Crime counts by agency with DV breakdown."""
    return queries.get_agencies(year_min, year_max, crime_against)


@mcp.tool()
def get_victims(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    """Victim demographics: age, race, sex by crime category."""
    return queries.get_victims(year_min, year_max, crime_against)


@mcp.tool()
def get_domestic_violence(
    year_min: int = 2021, year_max: int = 2025,
    agency: str | None = None,
) -> list[dict]:
    """Monthly domestic violence trends by agency and victim sex."""
    return queries.get_domestic_violence(year_min, year_max, agency)


@mcp.tool()
def get_temporal_patterns(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    """Day-of-week and month crime patterns."""
    return queries.get_temporal_patterns(year_min, year_max, crime_against)


@mcp.tool()
def get_cities(
    year_min: int = 2021, year_max: int = 2025,
    crime_against: str | None = None,
) -> list[dict]:
    """Crime counts by city."""
    return queries.get_cities(year_min, year_max, crime_against)


@mcp.tool()
def get_arrests(
    year_min: int = 2021, year_max: int = 2025,
    agency: str | None = None,
) -> list[dict]:
    """Group B arrest trends: DUI, disorderly conduct, vagrancy, trespass."""
    return queries.get_arrests(year_min, year_max, agency)


@mcp.tool()
def get_calls_for_service(
    year_min: int = 2015, year_max: int = 2026,
    priority: int | None = None,
) -> list[dict]:
    """Monthly call-for-service volume (2015-present). Filter by priority."""
    return queries.get_calls_for_service(year_min, year_max, priority)


@mcp.tool()
def get_calls_by_beat(
    year_min: int = 2015, year_max: int = 2026,
    priority: int | None = None,
) -> list[dict]:
    """Top 100 beats by call volume."""
    return queries.get_calls_by_beat(year_min, year_max, priority)


@mcp.tool()
def get_calls_temporal(priority: int | None = None) -> list[dict]:
    """Day-of-week and hour patterns for calls for service."""
    return queries.get_calls_temporal(priority)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
