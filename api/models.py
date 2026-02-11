"""Pydantic response models for the API."""

from __future__ import annotations

from pydantic import BaseModel


class FilterOptions(BaseModel):
    years: list[int]
    agencies: list[str]
    crime_categories: list[str]
    cities: list[str]


class YearlySummary(BaseModel):
    year: int
    total: int
    person_crimes: float | None = None
    property_crimes: float | None = None
    society_crimes: float | None = None
    dv_total: float | None = None
    stolen_vehicle_total: float | None = None


class TrendPoint(BaseModel):
    month_start: str | None = None
    crime_against: str | None = None
    total_incidents: int


class CrimeType(BaseModel):
    offense_group: str | None = None
    offense_description: str | None = None
    crime_against: str | None = None
    count: int


class GeographyRow(BaseModel):
    zip_code: str | None = None
    city: str | None = None
    crime_against: str | None = None
    count: int


class AgencyRow(BaseModel):
    agency_short: str
    crime_against: str | None = None
    count: int
    dv_count: int | None = None


class VictimRow(BaseModel):
    age_bin: str | None = None
    victim_race: str | None = None
    victim_sex: str | None = None
    crime_against: str | None = None
    count: int


class DVRow(BaseModel):
    agency: str | None = None
    offense_group: str | None = None
    victim_sex: str | None = None
    month_start: str | None = None
    count: int


class TemporalRow(BaseModel):
    dow: int
    month: int | None = None
    crime_against: str | None = None
    count: int


class CityRow(BaseModel):
    city: str
    crime_against: str | None = None
    count: int


class ArrestRow(BaseModel):
    offense_description: str
    agency_short: str
    month_start: str | None = None
    count: int


class CFSRow(BaseModel):
    month_start: str | None = None
    priority: int | None = None
    total_calls: int


class CFSBeatRow(BaseModel):
    beat: str | None = None
    priority: int | None = None
    total_calls: int


class CFSTemporalRow(BaseModel):
    dow: int
    hour: int
    priority: int | None = None
    total_calls: int
