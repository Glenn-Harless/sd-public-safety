"""San Diego Public Safety & Crime Patterns Dashboard."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

# ── Paths ──────────────────────────────────────────────────────────────
_AGG = "data/aggregated"
_PROC = "data/processed"
_root = Path(__file__).resolve().parent.parent
if (_root / _AGG).exists():
    _AGG = str(_root / _AGG)
if (_root / _PROC).exists():
    _PROC = str(_root / _PROC)

CHART_COLOR = "#83c9ff"

# ── Page config ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SD Public Safety",
    page_icon="🔒",
    layout="wide",
)
st.title("San Diego Public Safety & Crime Patterns")


# ── Helpers ────────────────────────────────────────────────────────────
def query(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def _fmt(n: int | float) -> str:
    if pd.isna(n):
        return "N/A"
    return f"{int(n):,}"


# ── Sidebar filters ───────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _sidebar_options():
    years = query(f"SELECT DISTINCT year FROM '{_AGG}/crime_by_agency.parquet' ORDER BY year")["year"].tolist()
    agencies = query(f"SELECT DISTINCT agency_short FROM '{_AGG}/crime_by_agency.parquet' ORDER BY agency_short")["agency_short"].tolist()
    categories = query(f"SELECT DISTINCT crime_against FROM '{_AGG}/crime_by_agency.parquet' WHERE crime_against IS NOT NULL ORDER BY crime_against")["crime_against"].tolist()
    cities = query(f"""
        SELECT DISTINCT city FROM '{_AGG}/crime_by_city.parquet'
        WHERE city IS NOT NULL
        ORDER BY city
    """)["city"].tolist()
    return years, agencies, categories, cities


years, agencies, categories, cities = _sidebar_options()

st.sidebar.header("Filters")
year_range = st.sidebar.slider(
    "Year Range",
    min_value=min(years), max_value=max(years),
    value=(min(years), max(y for y in years if y <= 2025)),
    help="Filter crime data by year range",
)
selected_agencies = st.sidebar.multiselect(
    "Agency", agencies,
    help="Filter by law enforcement agency",
)
selected_categories = st.sidebar.multiselect(
    "Crime Category", categories,
    help="People, Property, or Society",
)
selected_cities = st.sidebar.multiselect(
    "City", cities,
    help="Filter by city (within SD County)",
)


# ── WHERE clause builder ──────────────────────────────────────────────
def _where_clause(
    yr=year_range,
    agencies_list=selected_agencies,
    categories_list=selected_categories,
    cities_list=selected_cities,
    *,
    has_agency: bool = True,
    has_crime_against: bool = True,
    has_city: bool = False,
    year_col: str = "year",
) -> str:
    parts: list[str] = []
    parts.append(f"{year_col} >= {yr[0]}")
    parts.append(f"{year_col} <= {yr[1]}")
    if agencies_list and has_agency:
        escaped = ", ".join(f"'{a.replace(chr(39), chr(39)*2)}'" for a in agencies_list)
        parts.append(f"agency_short IN ({escaped})")
    if categories_list and has_crime_against:
        escaped = ", ".join(f"'{c.replace(chr(39), chr(39)*2)}'" for c in categories_list)
        parts.append(f"crime_against IN ({escaped})")
    if cities_list and has_city:
        escaped = ", ".join(f"'{c.replace(chr(39), chr(39)*2)}'" for c in cities_list)
        parts.append(f"city IN ({escaped})")
    return "WHERE " + " AND ".join(parts) if parts else ""


WHERE = _where_clause()
WHERE_NO_AGENCY = _where_clause(has_agency=False)
WHERE_NO_CRIME = _where_clause(has_crime_against=False)
WHERE_NO_BOTH = _where_clause(has_agency=False, has_crime_against=False)

DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# ── Tabs ───────────────────────────────────────────────────────────────
tab_overview, tab_map, tab_trends, tab_cfs, tab_geo, tab_types, tab_equity = st.tabs([
    "Overview", "Map", "Crime Trends", "Calls for Service",
    "Geographic", "Crime Types", "Equity",
])


# ═══════════════════════════════════════════════════════════════════════
# TAB 1: Overview
# ═══════════════════════════════════════════════════════════════════════
with tab_overview:
    summary = query(f"""
        SELECT year, total, person_crimes, property_crimes, society_crimes,
               dv_total, stolen_vehicle_total
        FROM '{_AGG}/yearly_summary.parquet'
        WHERE year >= {year_range[0]} AND year <= {year_range[1]}
        ORDER BY year
    """)

    if not summary.empty:
        totals = summary.sum()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Incidents", _fmt(totals["total"]))
        c2.metric("Crimes Against People", _fmt(totals["person_crimes"]))
        c3.metric("Property Crimes", _fmt(totals["property_crimes"]))
        c4.metric("Society Crimes", _fmt(totals["society_crimes"]))

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Domestic Violence", _fmt(totals["dv_total"]))
        c6.metric("Stolen Vehicles", _fmt(totals["stolen_vehicle_total"]))
        latest = summary.iloc[-1]
        if len(summary) >= 2:
            prev = summary.iloc[-2]
            yoy = (latest["total"] - prev["total"]) / prev["total"] * 100
            c7.metric("Latest Year", _fmt(latest["total"]), f"{yoy:+.1f}% YoY")
        else:
            c7.metric("Latest Year", _fmt(latest["total"]))
        c8.metric("Years Covered", f"{year_range[0]}-{year_range[1]}")

    # Top offenses
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 Offenses")
        top_offenses = query(f"""
            SELECT offense_description, SUM(count) AS count
            FROM '{_AGG}/crime_by_type.parquet' {WHERE_NO_AGENCY}
            GROUP BY offense_description
            ORDER BY count DESC LIMIT 10
        """)
        if not top_offenses.empty:
            fig = px.bar(top_offenses, x="count", y="offense_description",
                         orientation="h", color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title="", xaxis_title="Incidents")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Agency Breakdown")
        agency_df = query(f"""
            SELECT agency_short, SUM(count) AS count
            FROM '{_AGG}/crime_by_agency.parquet' {WHERE}
            GROUP BY agency_short
            ORDER BY count DESC
        """)
        if not agency_df.empty:
            fig = px.bar(agency_df, x="count", y="agency_short",
                         orientation="h", color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title="", xaxis_title="Incidents")
            st.plotly_chart(fig, use_container_width=True)

    # Crime category split
    st.subheader("Crime Category Split by Year")
    if not summary.empty:
        cat_df = summary.melt(
            id_vars=["year"],
            value_vars=["person_crimes", "property_crimes", "society_crimes"],
            var_name="category", value_name="count",
        )
        cat_df["category"] = cat_df["category"].str.replace("_crimes", "").str.title()
        fig = px.bar(cat_df, x="year", y="count", color="category", barmode="group")
        fig.update_layout(xaxis_title="Year", yaxis_title="Incidents")
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 2: Map
# ═══════════════════════════════════════════════════════════════════════
with tab_map:
    st.subheader("Crime Heatmap")
    st.caption("County-wide view. Random sample of up to 200K points for performance.")

    map_where = _where_clause(has_agency=False, has_city=False)
    # map_points uses "agency" not "agency_short"
    if selected_agencies:
        escaped = ", ".join(f"'{a.replace(chr(39), chr(39)*2)}'" for a in selected_agencies)
        map_where = map_where.replace("agency_short IN", "agency IN") if "agency_short IN" in map_where else map_where

    map_df = query(f"""
        SELECT lat, lng FROM '{_AGG}/map_points.parquet'
        {map_where}
        ORDER BY RANDOM() LIMIT 200000
    """)

    if not map_df.empty:
        layer = pdk.Layer(
            "HeatmapLayer",
            data=map_df,
            get_position=["lng", "lat"],
            radiusPixels=30,
            intensity=1,
            threshold=0.05,
            opacity=0.7,
        )
        view = pdk.ViewState(
            latitude=32.85, longitude=-117.1, zoom=9.5, pitch=0,
        )
        st.pydeck_chart(pdk.Deck(
            layers=[layer], initial_view_state=view, map_style="light",
        ))
        st.caption(f"Showing {len(map_df):,} points")
    else:
        st.info("No map data available for selected filters.")


# ═══════════════════════════════════════════════════════════════════════
# TAB 3: Crime Trends
# ═══════════════════════════════════════════════════════════════════════
with tab_trends:
    st.subheader("Monthly Crime Trends")
    WHERE_MONTHLY = _where_clause(year_col="YEAR(month_start)")
    monthly = query(f"""
        SELECT month_start, crime_against,
               SUM(total_incidents) AS total_incidents
        FROM '{_AGG}/crime_overview_monthly.parquet' {WHERE_MONTHLY}
        GROUP BY month_start, crime_against
        ORDER BY month_start
    """)

    if not monthly.empty:
        monthly["month_start"] = pd.to_datetime(monthly["month_start"])
        fig = px.line(monthly, x="month_start", y="total_incidents",
                      color="crime_against", labels={"month_start": "Month", "total_incidents": "Incidents"})
        st.plotly_chart(fig, use_container_width=True)

    # YoY comparison
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Year-over-Year")
        yoy_df = query(f"""
            SELECT YEAR(month_start) AS year, crime_against, SUM(total_incidents) AS total
            FROM '{_AGG}/crime_overview_monthly.parquet' {WHERE_MONTHLY}
            GROUP BY YEAR(month_start), crime_against
            ORDER BY year
        """)
        if not yoy_df.empty:
            fig = px.bar(yoy_df, x="year", y="total", color="crime_against", barmode="group")
            fig.update_layout(xaxis_title="Year", yaxis_title="Incidents")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Seasonal Patterns")
        seasonal = query(f"""
            SELECT month, crime_against, SUM(count) AS count
            FROM '{_AGG}/temporal_patterns.parquet' {WHERE_NO_AGENCY}
            GROUP BY month, crime_against
            ORDER BY month
        """)
        if not seasonal.empty:
            seasonal["month_name"] = seasonal["month"].map(lambda m: MONTH_NAMES[int(m) - 1] if pd.notna(m) and 1 <= int(m) <= 12 else "?")
            fig = px.line(seasonal, x="month_name", y="count", color="crime_against")
            fig.update_layout(xaxis_title="Month", yaxis_title="Incidents")
            st.plotly_chart(fig, use_container_width=True)

    # Day of week heatmap
    st.subheader("Day-of-Week Patterns")
    dow_df = query(f"""
        SELECT dow, month, SUM(count) AS count
        FROM '{_AGG}/temporal_patterns.parquet' {WHERE_NO_AGENCY}
        GROUP BY dow, month
    """)
    if not dow_df.empty:
        dow_df["day_name"] = dow_df["dow"].map(lambda d: DAY_NAMES[int(d)] if pd.notna(d) and 0 <= int(d) <= 6 else "?")
        dow_df["month_name"] = dow_df["month"].map(lambda m: MONTH_NAMES[int(m) - 1] if pd.notna(m) and 1 <= int(m) <= 12 else "?")
        pivot = dow_df.pivot_table(index="day_name", columns="month_name", values="count", aggfunc="sum")
        pivot = pivot.reindex(DAY_NAMES, axis=0)
        pivot = pivot.reindex(MONTH_NAMES, axis=1)
        fig = px.imshow(pivot, color_continuous_scale="Blues",
                        labels=dict(x="Month", y="Day", color="Incidents"))
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 4: Calls for Service
# ═══════════════════════════════════════════════════════════════════════
with tab_cfs:
    st.subheader("Calls for Service (2015-present)")

    cfs_yr = st.slider("CFS Year Range", 2015, 2026, (2015, 2025), key="cfs_yr")

    cfs_where = f"WHERE year >= {cfs_yr[0]} AND year <= {cfs_yr[1]}"

    # Volume over time
    cfs_monthly = query(f"""
        SELECT month_start, SUM(total_calls) AS total_calls
        FROM '{_AGG}/cfs_monthly.parquet'
        {cfs_where} AND month_start IS NOT NULL
        GROUP BY month_start ORDER BY month_start
    """)
    if not cfs_monthly.empty:
        cfs_monthly["month_start"] = pd.to_datetime(cfs_monthly["month_start"])
        fig = px.line(cfs_monthly, x="month_start", y="total_calls",
                      color_discrete_sequence=[CHART_COLOR])
        fig.update_layout(xaxis_title="Month", yaxis_title="Calls")
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Priority Breakdown")
        priority_df = query(f"""
            SELECT priority, SUM(total_calls) AS total_calls
            FROM '{_AGG}/cfs_monthly.parquet'
            {cfs_where}
            GROUP BY priority ORDER BY priority
        """)
        if not priority_df.empty:
            priority_df["priority"] = priority_df["priority"].astype(str)
            fig = px.bar(priority_df, x="priority", y="total_calls",
                         color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(xaxis_title="Priority", yaxis_title="Total Calls")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Call Types")
        call_types = query(f"""
            SELECT call_type_desc, SUM(total_calls) AS total_calls
            FROM '{_AGG}/cfs_monthly.parquet'
            {cfs_where}
            GROUP BY call_type_desc
            ORDER BY total_calls DESC LIMIT 15
        """)
        if not call_types.empty:
            fig = px.bar(call_types, x="total_calls", y="call_type_desc",
                         orientation="h", color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title="", xaxis_title="Calls")
            st.plotly_chart(fig, use_container_width=True)

    # Beat-level patterns
    st.subheader("Top Beats by Call Volume")
    beats = query(f"""
        SELECT beat, SUM(total_calls) AS total_calls
        FROM '{_AGG}/cfs_by_beat.parquet'
        {cfs_where}
        GROUP BY beat ORDER BY total_calls DESC LIMIT 20
    """)
    if not beats.empty:
        fig = px.bar(beats, x="beat", y="total_calls",
                     color_discrete_sequence=[CHART_COLOR])
        fig.update_layout(xaxis_title="Beat", yaxis_title="Total Calls")
        st.plotly_chart(fig, use_container_width=True)

    # Day/hour heatmap
    st.subheader("Day-of-Week / Hour Heatmap")
    cfs_temporal = query(f"""
        SELECT dow, hour, SUM(total_calls) AS total_calls
        FROM '{_AGG}/cfs_temporal.parquet'
        GROUP BY dow, hour
    """)
    if not cfs_temporal.empty:
        cfs_temporal["day_name"] = cfs_temporal["dow"].map(lambda d: DAY_NAMES[int(d)] if pd.notna(d) and 0 <= int(d) <= 6 else "?")
        hour_labels = [f"{h % 12 or 12}{'am' if h < 12 else 'pm'}" for h in range(24)]
        cfs_temporal["hour_label"] = cfs_temporal["hour"].map(lambda h: hour_labels[int(h)] if pd.notna(h) and 0 <= int(h) <= 23 else "?")
        pivot = cfs_temporal.pivot_table(index="day_name", columns="hour_label", values="total_calls", aggfunc="sum")
        pivot = pivot.reindex(DAY_NAMES, axis=0)
        pivot = pivot.reindex(hour_labels, axis=1)
        fig = px.imshow(pivot, color_continuous_scale="Blues",
                        labels=dict(x="Hour", y="Day", color="Calls"))
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 5: Geographic
# ═══════════════════════════════════════════════════════════════════════
with tab_geo:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top ZIP Codes by Crime Count")
        top_zips = query(f"""
            SELECT zip_code, city, SUM(count) AS count
            FROM '{_AGG}/crime_by_zip.parquet' {WHERE_NO_AGENCY}
            GROUP BY zip_code, city
            ORDER BY count DESC LIMIT 20
        """)
        if not top_zips.empty:
            top_zips["label"] = top_zips["zip_code"] + " (" + top_zips["city"].fillna("") + ")"
            fig = px.bar(top_zips, x="count", y="label", orientation="h",
                         color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title="", xaxis_title="Incidents")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Bottom ZIP Codes (lowest crime)")
        bottom_zips = query(f"""
            SELECT zip_code, city, SUM(count) AS count
            FROM '{_AGG}/crime_by_zip.parquet' {WHERE_NO_AGENCY}
            GROUP BY zip_code, city
            HAVING SUM(count) >= 10
            ORDER BY count ASC LIMIT 20
        """)
        if not bottom_zips.empty:
            bottom_zips["label"] = bottom_zips["zip_code"] + " (" + bottom_zips["city"].fillna("") + ")"
            fig = px.bar(bottom_zips, x="count", y="label", orientation="h",
                         color_discrete_sequence=["#2a6496"])
            fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title="", xaxis_title="Incidents")
            st.plotly_chart(fig, use_container_width=True)

    # City comparison
    st.subheader("City Comparison")
    city_df = query(f"""
        SELECT city, crime_against, SUM(count) AS count
        FROM '{_AGG}/crime_by_city.parquet' {WHERE_NO_AGENCY}
        GROUP BY city, crime_against
        ORDER BY count DESC
    """)
    if not city_df.empty:
        # Top 15 cities by total
        top_cities = city_df.groupby("city")["count"].sum().nlargest(15).index
        city_filtered = city_df[city_df["city"].isin(top_cities)]
        fig = px.bar(city_filtered, x="city", y="count", color="crime_against", barmode="stack")
        fig.update_layout(xaxis_title="City", yaxis_title="Incidents")
        st.plotly_chart(fig, use_container_width=True)

    # CFS by beat
    st.subheader("CFS by Beat (Top 20)")
    cfs_beats = query(f"""
        SELECT beat, SUM(total_calls) AS total_calls
        FROM '{_AGG}/cfs_by_beat.parquet'
        WHERE year >= {year_range[0]} AND year <= {year_range[1]}
        GROUP BY beat ORDER BY total_calls DESC LIMIT 20
    """)
    if not cfs_beats.empty:
        fig = px.bar(cfs_beats, x="beat", y="total_calls",
                     color_discrete_sequence=[CHART_COLOR])
        fig.update_layout(xaxis_title="Beat", yaxis_title="Total Calls")
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 6: Crime Types
# ═══════════════════════════════════════════════════════════════════════
with tab_types:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Crime Category Breakdown")
        cat_totals = query(f"""
            SELECT crime_against, SUM(count) AS count
            FROM '{_AGG}/crime_by_type.parquet' {WHERE_NO_AGENCY}
            GROUP BY crime_against
            ORDER BY count DESC
        """)
        if not cat_totals.empty:
            fig = px.pie(cat_totals, values="count", names="crime_against",
                         color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Offense Groups")
        groups = query(f"""
            SELECT offense_group, SUM(count) AS count
            FROM '{_AGG}/crime_by_type.parquet' {WHERE_NO_AGENCY}
            WHERE offense_group IS NOT NULL
            GROUP BY offense_group
            ORDER BY count DESC LIMIT 15
        """)
        if not groups.empty:
            fig = px.bar(groups, x="count", y="offense_group", orientation="h",
                         color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title="", xaxis_title="Incidents")
            st.plotly_chart(fig, use_container_width=True)

    # Trend by category
    st.subheader("Trend by Crime Category")
    cat_trend = query(f"""
        SELECT year, crime_against, SUM(count) AS count
        FROM '{_AGG}/crime_by_type.parquet' {WHERE_NO_AGENCY}
        GROUP BY year, crime_against
        ORDER BY year
    """)
    if not cat_trend.empty:
        fig = px.line(cat_trend, x="year", y="count", color="crime_against")
        fig.update_layout(xaxis_title="Year", yaxis_title="Incidents")
        st.plotly_chart(fig, use_container_width=True)

    # Detail table
    st.subheader("Offense Detail")
    detail = query(f"""
        SELECT offense_description, crime_against, SUM(count) AS count
        FROM '{_AGG}/crime_by_type.parquet' {WHERE_NO_AGENCY}
        GROUP BY offense_description, crime_against
        ORDER BY count DESC
    """)
    if not detail.empty:
        st.dataframe(detail, use_container_width=True, hide_index=True,
                     column_config={"count": st.column_config.NumberColumn("Incidents", format="%d")})

    # Group B arrests
    st.subheader("Arrests: DUI / Disorderly Conduct / Vagrancy")
    arrests_where = _where_clause(has_crime_against=False)
    arrests_df = query(f"""
        SELECT offense_description, month_start, SUM(count) AS count
        FROM '{_AGG}/arrests_by_type.parquet' {arrests_where}
        GROUP BY offense_description, month_start
        ORDER BY month_start
    """)
    if not arrests_df.empty:
        arrests_df["month_start"] = pd.to_datetime(arrests_df["month_start"])
        fig = px.line(arrests_df, x="month_start", y="count", color="offense_description")
        fig.update_layout(xaxis_title="Month", yaxis_title="Arrests")
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 7: Equity
# ═══════════════════════════════════════════════════════════════════════
with tab_equity:
    # Victim demographics
    st.subheader("Victim Demographics")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**By Age**")
        age_df = query(f"""
            SELECT age_bin, SUM(count) AS count
            FROM '{_AGG}/victim_demographics.parquet' {WHERE_NO_AGENCY}
            WHERE age_bin != 'Unknown'
            GROUP BY age_bin ORDER BY count DESC
        """)
        if not age_df.empty:
            fig = px.bar(age_df, x="age_bin", y="count", color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(xaxis_title="Age Group", yaxis_title="Victims")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**By Race**")
        race_df = query(f"""
            SELECT victim_race, SUM(count) AS count
            FROM '{_AGG}/victim_demographics.parquet' {WHERE_NO_AGENCY}
            WHERE victim_race IS NOT NULL
            GROUP BY victim_race ORDER BY count DESC
        """)
        if not race_df.empty:
            fig = px.bar(race_df, x="victim_race", y="count", color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(xaxis_title="Race", yaxis_title="Victims")
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("**By Sex**")
        sex_df = query(f"""
            SELECT victim_sex, SUM(count) AS count
            FROM '{_AGG}/victim_demographics.parquet' {WHERE_NO_AGENCY}
            WHERE victim_sex IS NOT NULL
            GROUP BY victim_sex ORDER BY count DESC
        """)
        if not sex_df.empty:
            fig = px.pie(sex_df, values="count", names="victim_sex",
                         color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.markdown("**Victim Category by Year**")
        vic_trend = query(f"""
            SELECT year, crime_against, SUM(count) AS count
            FROM '{_AGG}/victim_demographics.parquet' {WHERE_NO_AGENCY}
            GROUP BY year, crime_against
            ORDER BY year
        """)
        if not vic_trend.empty:
            fig = px.line(vic_trend, x="year", y="count", color="crime_against")
            fig.update_layout(xaxis_title="Year", yaxis_title="Victims")
            st.plotly_chart(fig, use_container_width=True)

    # Domestic violence trends
    st.subheader("Domestic Violence Trends by Agency")
    dv_where = _where_clause(has_crime_against=False)
    if selected_agencies:
        # DV table uses "agency" not "agency_short"
        escaped = ", ".join(f"'{a.replace(chr(39), chr(39)*2)}'" for a in selected_agencies)
        dv_where = dv_where.replace("agency_short IN", "agency IN") if "agency_short IN" in dv_where else dv_where
    dv_df = query(f"""
        SELECT agency, month_start, SUM(count) AS count
        FROM '{_AGG}/domestic_violence.parquet' {dv_where}
        GROUP BY agency, month_start
        ORDER BY month_start
    """)
    if not dv_df.empty:
        dv_df["month_start"] = pd.to_datetime(dv_df["month_start"])
        fig = px.line(dv_df, x="month_start", y="count", color="agency")
        fig.update_layout(xaxis_title="Month", yaxis_title="DV Incidents")
        st.plotly_chart(fig, use_container_width=True)

    # ZIP rate disparity
    st.subheader("ZIP Code Crime Disparity")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Highest Crime ZIPs**")
        high_zips = query(f"""
            SELECT zip_code, city, SUM(count) AS count
            FROM '{_AGG}/crime_by_zip.parquet' {WHERE_NO_AGENCY}
            GROUP BY zip_code, city
            ORDER BY count DESC LIMIT 10
        """)
        if not high_zips.empty:
            high_zips["label"] = high_zips["zip_code"] + " (" + high_zips["city"].fillna("") + ")"
            fig = px.bar(high_zips, x="count", y="label", orientation="h",
                         color_discrete_sequence=[CHART_COLOR])
            fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Lowest Crime ZIPs**")
        low_zips = query(f"""
            SELECT zip_code, city, SUM(count) AS count
            FROM '{_AGG}/crime_by_zip.parquet' {WHERE_NO_AGENCY}
            GROUP BY zip_code, city
            HAVING SUM(count) >= 10
            ORDER BY count ASC LIMIT 10
        """)
        if not low_zips.empty:
            low_zips["label"] = low_zips["zip_code"] + " (" + low_zips["city"].fillna("") + ")"
            fig = px.bar(low_zips, x="count", y="label", orientation="h",
                         color_discrete_sequence=["#2a6496"])
            fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
