from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

APP_TITLE = "Interactive GB Grid Impact Dashboard for Wylfa SMR Integration"
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "outputs" / "dashboard" / "objective3_smr_integration" / "data"
DATA_DIR = Path(os.environ.get("RRSMR_DASHBOARD_DATA_DIR", DEFAULT_DATA_DIR)).expanduser().resolve()
HOURLY_PATH = DATA_DIR / "hourly_metrics_dashboard.parquet"
ANNUAL_PATH = DATA_DIR / "annual_summary.csv"
PERIOD_PATH = DATA_DIR / "period_summary.csv"
LOW_WIND_RANKINGS_PATH = DATA_DIR / "low_wind_case_study_selection_rankings.csv"
LOW_WIND_CASE_DAY_PATH = DATA_DIR / "low_wind_case_study_pressure_day.csv"
QA_CHECKS_PATH = DATA_DIR / "qa_checks.csv"
SENSITIVITY_DEFINITIONS_PATH = DATA_DIR / "sensitivity_definitions.csv"
SMR_ASSUMPTIONS_PATH = DATA_DIR / "smr_assumptions.csv"

BASE_CASE = "staggered_commissioning"
STRESS_CASE = "simultaneous_commissioning"
BASE_WEATHER = "average_wind"
LOW_WIND = "low_wind"
CASE_LABELS = {
    BASE_CASE: "Staggered commissioning",
    STRESS_CASE: "Simultaneous commissioning stress-test",
}
SHORT_CASE_LABELS = {
    BASE_CASE: "Staggered",
    STRESS_CASE: "Simultaneous stress-test",
}

CLIMATE_MEMBER_LABELS = {
    "member_12": "Cooler demand-climate case",
    "member_06": "Mid-range demand-climate case",
    "member_13": "Warmer demand-climate case",
}

CLIMATE_MEMBER_HELP = {
    "member_12": (
        "UKCP18 member 12. Coolest of the selected future climate realisations "
        "over 2030–2045, based on mean daily temperature across England & Wales "
        "and Scotland. This affects the hourly and seasonal demand shape; it is "
        "not a separate FES pathway."
    ),
    "member_06": (
        "UKCP18 member 06. Mid-range selected future climate realisation over "
        "2030–2045, based on mean daily temperature across England & Wales and "
        "Scotland. This affects the hourly and seasonal demand shape; it is not "
        "a separate FES pathway."
    ),
    "member_13": (
        "UKCP18 member 13. Warmest of the selected future climate realisations "
        "over 2030–2045, based on mean daily temperature across England & Wales "
        "and Scotland. This affects the hourly and seasonal demand shape; it is "
        "not a separate FES pathway."
    ),
}
WEATHER_ROLE_LABELS = {
    BASE_WEATHER: "Average-wind supply case",
    LOW_WIND: "Low-wind supply stress case",
    "high_wind": "High-wind supply sensitivity",
}
WEATHER_ROLE_DEFINITIONS = {
    BASE_WEATHER: "Central supply-side weather case used as the main model view for wind and renewable output.",
    LOW_WIND: "Stressed supply-side case used to test system support when wind generation is low. In the dashboard data, low-wind support is also flagged where wind output is at or below the inferred low-wind threshold.",
    "high_wind": "Higher-wind supply sensitivity, included where available in upstream integration outputs.",
}
KPI_DEFINITIONS = {
    "Cumulative SMR energy": "Total electrical energy delivered by the three modelled SMR units over the selected 2030–2045 scenario slice.",
    "Gas displacement proxy": "Reduction in non-negative residual demand after SMR output is added. This is a simplified proxy for gas-fired generation that may no longer be required, not a full market dispatch forecast.",
    "Average residual reduction": "Average hourly reduction in residual demand caused by SMR output. In this rule-based model it broadly represents the average SMR contribution to meeting residual demand.",
    "Total surplus hours": "Number of hours where residual demand after SMR output falls below zero. This is an oversupply or curtailment-risk proxy, not a curtailment dispatch model.",
    "Low-wind support hours": "Hours flagged as low-wind where SMR output is positive and reduces the modelled gas/balancing requirement.",
}

st.set_page_config(
    page_title="SMR Integration Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)


def get_configured_password() -> str | None:
    """Return a password from Streamlit secrets or environment variables.

    For local demonstration, the app can run without a password. For client
    sharing, configure either st.secrets["dashboard_password"] or the
    DASHBOARD_PASSWORD environment variable.
    """
    try:
        secret_password = st.secrets.get("dashboard_password")  # type: ignore[attr-defined]
        if secret_password:
            return str(secret_password)
    except Exception:
        pass
    env_password = os.environ.get("DASHBOARD_PASSWORD")
    return env_password if env_password else None


def require_access() -> None:
    password = get_configured_password()
    if not password:
        st.sidebar.warning(
            "No access password is configured. The app is running in local/demo mode. "
            "Set `dashboard_password` in Streamlit secrets before sharing externally."
        )
        return

    if st.session_state.get("authenticated") is True:
        return

    st.title(APP_TITLE)
    st.info("This dashboard is access-controlled. Enter the project access code to continue.")
    entered = st.text_input("Access code", type="password")
    if st.button("Enter dashboard", type="primary"):
        if entered == password:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect access code.")
    st.stop()


@st.cache_data(show_spinner=False)
def load_annual() -> pd.DataFrame:
    return pd.read_csv(ANNUAL_PATH)


@st.cache_data(show_spinner=False)
def load_period() -> pd.DataFrame:
    return pd.read_csv(PERIOD_PATH)


@st.cache_data(show_spinner="Loading hourly dashboard data...")
def load_hourly() -> pd.DataFrame:
    df = pd.read_parquet(HOURLY_PATH)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df["date"] = df["timestamp_utc"].dt.date
    return df


@st.cache_data(show_spinner=False)
def load_low_wind_rankings() -> pd.DataFrame:
    df = pd.read_csv(LOW_WIND_RANKINGS_PATH)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


@st.cache_data(show_spinner=False)
def load_low_wind_case_day() -> pd.DataFrame:
    df = pd.read_csv(LOW_WIND_CASE_DAY_PATH)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    return df


@st.cache_data(show_spinner=False)
def load_qa_checks() -> pd.DataFrame:
    return pd.read_csv(QA_CHECKS_PATH)


@st.cache_data(show_spinner=False)
def load_smr_assumptions() -> pd.DataFrame:
    return pd.read_csv(SMR_ASSUMPTIONS_PATH)


@st.cache_data(show_spinner=False)
def load_sensitivity_definitions() -> pd.DataFrame:
    return pd.read_csv(SENSITIVITY_DEFINITIONS_PATH)


def read_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return "File not found in dashboard package."

def require_dashboard_data() -> None:
    required = [
        HOURLY_PATH,
        ANNUAL_PATH,
        PERIOD_PATH,
        LOW_WIND_RANKINGS_PATH,
        LOW_WIND_CASE_DAY_PATH,
        QA_CHECKS_PATH,
        SENSITIVITY_DEFINITIONS_PATH,
        SMR_ASSUMPTIONS_PATH,
    ]
    missing = [path for path in required if not path.exists()]
    if not missing:
        return

    st.title(APP_TITLE)
    st.error("Dashboard data has not been generated yet.")
    st.markdown("Run this from the repository root:")
    st.code(
        "python dashboard/objective3_smr_integration/scripts/build_dashboard_data.py",
        language="text",
    )
    st.markdown("Expected data directory:")
    st.code(str(DATA_DIR), language="text")
    st.markdown("Missing files:")
    for path in missing:
        st.markdown(f"- `{path}`")
    st.stop()


def fmt_twh(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.2f} TWh"


def fmt_mw(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.0f} MW"


def fmt_int(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{int(value):,}"


def label_case(case: str) -> str:
    return CASE_LABELS.get(case, case)


def short_case(case: str) -> str:
    return SHORT_CASE_LABELS.get(str(case), str(case))


def label_climate_member(member: str) -> str:
    member = str(member)
    return f"{CLIMATE_MEMBER_LABELS.get(member, member)} ({member})"


def label_weather_role(role: str) -> str:
    role = str(role)
    return WEATHER_ROLE_LABELS.get(role, role.replace("_", " ").title())


def safe_map_labels(series: pd.Series, mapping: dict[str, str]) -> pd.Series:
    # Cast away pandas categorical dtype before fillna. This avoids the
    # "Cannot set a Categorical with another" error seen on pandas 2.x/3.x.
    raw = series.astype("string")
    mapped = raw.map(mapping)
    return mapped.fillna(raw).astype(str)


def add_case_label(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["smr_case_label"] = safe_map_labels(out["smr_case"], SHORT_CASE_LABELS)
    return out


def add_context_labels(df: pd.DataFrame) -> pd.DataFrame:
    out = add_case_label(df) if "smr_case" in df.columns else df.copy()
    if "climate_member" in out.columns:
        out["demand_climate_scenario"] = out["climate_member"].astype("string").map(
            lambda v: label_climate_member(v)
        )
    if "weather_year_role" in out.columns:
        out["supply_weather_case"] = out["weather_year_role"].astype("string").map(
            lambda v: label_weather_role(v)
        )
    return out


def get_options(df: pd.DataFrame, col: str) -> list[str]:
    return sorted([str(v) for v in df[col].dropna().unique()])


def sidebar_filters(annual: pd.DataFrame) -> dict[str, str]:
    st.sidebar.title("Dashboard controls")
    fes_options = get_options(annual, "fes_scenario")
    climate_options = get_options(annual, "climate_member")
    weather_options = get_options(annual, "weather_year_role")
    case_options = get_options(annual, "smr_case")

    filters = {
        "fes_scenario": st.sidebar.selectbox(
            "FES pathway",
            fes_options,
            index=0,
            help="Future Energy Scenarios pathway used to anchor demand and supply assumptions.",
        ),
        "climate_member": st.sidebar.selectbox(
            "Demand climate scenario",
            climate_options,
            index=0,
            format_func=label_climate_member,
            help="UKCP18-based climate realisation used on the demand side. A/B/C are display labels for the three selected model members.",
        ),
        "weather_year_role": st.sidebar.selectbox(
            "Supply/weather case",
            weather_options,
            index=weather_options.index(BASE_WEATHER) if BASE_WEATHER in weather_options else 0,
            format_func=label_weather_role,
            help="Supply-side wind/weather case used for renewable-output stress testing.",
        ),
        "smr_case": st.sidebar.selectbox(
            "SMR deployment case",
            case_options,
            index=case_options.index(BASE_CASE) if BASE_CASE in case_options else 0,
            format_func=label_case,
            help="Staggered commissioning is the main model; simultaneous commissioning is a stress-test sensitivity.",
        ),
    }
    st.sidebar.divider()
    st.sidebar.markdown("**Selected scenario**")
    st.sidebar.caption(f"FES pathway: {filters['fes_scenario']}")
    st.sidebar.caption(f"Demand climate: {label_climate_member(filters['climate_member'])}")
    st.sidebar.caption(f"Supply/weather: {label_weather_role(filters['weather_year_role'])}")
    st.sidebar.caption(f"SMR case: {label_case(filters['smr_case'])}")
    with st.sidebar.expander("What do these scenario labels mean?"):
        st.markdown(
            f"""
            - **Demand climate scenario:** {CLIMATE_MEMBER_HELP.get(filters['climate_member'], 'UKCP18-based demand climate realisation.')}
            - **Supply/weather case:** {WEATHER_ROLE_DEFINITIONS.get(filters['weather_year_role'], 'Supply-side weather sensitivity.')}
            - **SMR deployment case:** `Staggered` is the main phased build-out case; `Simultaneous stress-test` asks what changes if all three units are online from the first commissioning year.
            """
        )
    return filters

def page_header(title: str, caption: str | None = None) -> None:
    st.title(title)
    if caption:
        st.caption(caption)


def selected_period_row(period: pd.DataFrame, filters: dict[str, str]) -> pd.Series | None:
    mask = np.ones(len(period), dtype=bool)
    for key, value in filters.items():
        if key in period.columns:
            mask &= period[key].astype(str).eq(str(value))
    rows = period.loc[mask]
    if rows.empty:
        return None
    return rows.iloc[0]


def filter_annual(annual: pd.DataFrame, filters: dict[str, str], include_case: bool = True) -> pd.DataFrame:
    mask = (
        annual["fes_scenario"].astype(str).eq(filters["fes_scenario"])
        & annual["climate_member"].astype(str).eq(filters["climate_member"])
        & annual["weather_year_role"].astype(str).eq(filters["weather_year_role"])
    )
    if include_case:
        mask &= annual["smr_case"].astype(str).eq(filters["smr_case"])
    return annual.loc[mask].copy()


def filter_period_compare(period: pd.DataFrame, filters: dict[str, str]) -> pd.DataFrame:
    mask = (
        period["fes_scenario"].astype(str).eq(filters["fes_scenario"])
        & period["climate_member"].astype(str).eq(filters["climate_member"])
        & period["weather_year_role"].astype(str).eq(filters["weather_year_role"])
    )
    return add_context_labels(period.loc[mask].copy())


def plot_annual_trends(annual_subset: pd.DataFrame) -> None:
    df = add_context_labels(annual_subset)
    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(
            df,
            x="year",
            y="annual_smr_energy_twh",
            color="smr_case_label",
            markers=True,
            labels={
                "year": "Year",
                "annual_smr_energy_twh": "Annual SMR energy (TWh)",
                "smr_case_label": "SMR case",
            },
            title="Annual SMR delivered energy",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.line(
            df,
            x="year",
            y="annual_gas_displacement_twh",
            color="smr_case_label",
            markers=True,
            labels={
                "year": "Year",
                "annual_gas_displacement_twh": "Annual gas displacement proxy (TWh)",
                "smr_case_label": "SMR case",
            },
            title="Annual gas displacement proxy",
        )
        st.plotly_chart(fig, use_container_width=True)


def overview_page(annual: pd.DataFrame, period: pd.DataFrame, filters: dict[str, str]) -> None:
    page_header(
        APP_TITLE,
        "An interactive project dashboard for exploring the modelled GB grid impact of three Wylfa SMRs, 2030–2045.",
    )

    row = selected_period_row(period, filters)
    if row is None:
        st.error("No period summary row matches the current filter combination.")
        return

    st.subheader("Selected scenario summary")
    st.caption(
        f"Showing {filters['fes_scenario']} · {label_climate_member(filters['climate_member'])} · "
        f"{label_weather_role(filters['weather_year_role'])} · {label_case(filters['smr_case'])}"
    )
    kpi_cols = st.columns(5)
    kpi_cols[0].metric(
        "Cumulative SMR energy",
        fmt_twh(row["cumulative_smr_energy_twh"]),
        help=KPI_DEFINITIONS["Cumulative SMR energy"],
    )
    kpi_cols[1].metric(
        "Gas displacement proxy",
        fmt_twh(row["cumulative_gas_displacement_twh"]),
        help=KPI_DEFINITIONS["Gas displacement proxy"],
    )
    kpi_cols[2].metric(
        "Average residual reduction",
        fmt_mw(row["average_residual_demand_reduction_mw"]),
        help=KPI_DEFINITIONS["Average residual reduction"],
    )
    kpi_cols[3].metric(
        "Total surplus hours",
        fmt_int(row["total_surplus_hours"]),
        help=KPI_DEFINITIONS["Total surplus hours"],
    )
    kpi_cols[4].metric(
        "Low-wind support hours",
        fmt_int(row["total_low_wind_support_hours"]),
        help=KPI_DEFINITIONS["Low-wind support hours"],
    )

    with st.expander("Metric definitions", expanded=False):
        for metric, definition in KPI_DEFINITIONS.items():
            st.markdown(f"**{metric}:** {definition}")

    st.subheader("Base case versus stress-test")
    compare = filter_period_compare(period, filters)
    if compare.empty:
        st.warning("No comparison rows are available for this filter combination.")
    else:
        display_cols = [
            "smr_case_label",
            "cumulative_smr_energy_twh",
            "cumulative_gas_displacement_twh",
            "average_residual_demand_reduction_mw",
            "average_gas_displacement_proxy_mw",
            "total_surplus_hours",
            "total_low_wind_support_hours",
        ]
        st.dataframe(
            compare[display_cols].rename(
                columns={
                    "smr_case_label": "SMR case",
                    "cumulative_smr_energy_twh": "Cumulative SMR energy (TWh)",
                    "cumulative_gas_displacement_twh": "Cumulative gas displacement proxy (TWh)",
                    "average_residual_demand_reduction_mw": "Average residual reduction (MW)",
                    "average_gas_displacement_proxy_mw": "Average gas displacement proxy (MW)",
                    "total_surplus_hours": "Total surplus hours",
                    "total_low_wind_support_hours": "Low-wind support hours",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
        fig = px.bar(
            compare,
            x="smr_case_label",
            y="cumulative_gas_displacement_twh",
            labels={
                "smr_case_label": "SMR case",
                "cumulative_gas_displacement_twh": "Cumulative gas displacement proxy (TWh)",
            },
            title="Whole-period gas displacement proxy, 2030–2045",
        )
        st.plotly_chart(fig, use_container_width=True)

    annual_compare = filter_annual(annual, filters, include_case=False)
    st.subheader("Annual trend")
    plot_annual_trends(annual_compare)


def scenario_explorer_page(annual: pd.DataFrame, period: pd.DataFrame, filters: dict[str, str]) -> None:
    page_header(
        "Scenario explorer",
        "Compare annual results across SMR deployment cases for the selected FES pathway, climate member, and weather role.",
    )
    annual_subset = filter_annual(annual, filters, include_case=False)
    if annual_subset.empty:
        st.error("No annual data matches the selected filters.")
        return

    plot_annual_trends(annual_subset)

    df = add_context_labels(annual_subset)
    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(
            df,
            x="year",
            y="average_residual_after_mw",
            color="smr_case_label",
            markers=True,
            labels={
                "year": "Year",
                "average_residual_after_mw": "Average residual demand after SMR (MW)",
                "smr_case_label": "SMR case",
            },
            title="Average residual demand after SMR",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.line(
            df,
            x="year",
            y="surplus_hours_count",
            color="smr_case_label",
            markers=True,
            labels={
                "year": "Year",
                "surplus_hours_count": "Surplus hours",
                "smr_case_label": "SMR case",
            },
            title="Surplus / oversupply proxy hours",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Annual summary table")
    annual_display_cols = [
        "year",
        "fes_scenario",
        "demand_climate_scenario",
        "supply_weather_case",
        "smr_case_label",
        "annual_smr_energy_twh",
        "annual_gas_displacement_twh",
        "average_residual_before_mw",
        "average_residual_after_mw",
        "surplus_hours_count",
        "low_wind_support_hours",
    ]
    annual_display = df[[c for c in annual_display_cols if c in df.columns]].rename(
        columns={
            "year": "Year",
            "fes_scenario": "FES pathway",
            "demand_climate_scenario": "Demand climate scenario",
            "supply_weather_case": "Supply/weather case",
            "smr_case_label": "SMR case",
            "annual_smr_energy_twh": "Annual SMR energy (TWh)",
            "annual_gas_displacement_twh": "Annual gas displacement proxy (TWh)",
            "average_residual_before_mw": "Avg residual before SMR (MW)",
            "average_residual_after_mw": "Avg residual after SMR (MW)",
            "surplus_hours_count": "Surplus hours",
            "low_wind_support_hours": "Low-wind support hours",
        }
    )
    st.dataframe(annual_display, use_container_width=True, hide_index=True)


def select_year_and_range(hourly: pd.DataFrame) -> tuple[int, date, date]:
    years = sorted(hourly["year"].dropna().astype(int).unique())
    default_year = 2036 if 2036 in years else years[0]
    year = st.selectbox("Year", years, index=years.index(default_year))
    year_dates = hourly.loc[hourly["year"].eq(year), "date"]
    min_date = min(year_dates)
    max_date = max(year_dates)
    default_start = date(year, 1, 1)
    default_end = min(default_start + timedelta(days=13), max_date)
    selected_range = st.date_input(
        "Date range for hourly plots",
        value=(default_start, default_end),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start_date, end_date = selected_range
    else:
        start_date = selected_range if isinstance(selected_range, date) else default_start
        end_date = min(start_date + timedelta(days=13), max_date)
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    if (end_date - start_date).days > 60:
        st.warning("Hourly plots are capped to 60 days for readability.")
        end_date = start_date + timedelta(days=60)
    return year, start_date, end_date


def hourly_filter(hourly: pd.DataFrame, filters: dict[str, str], year: int | None = None, include_case: bool = False) -> pd.DataFrame:
    mask = (
        hourly["fes_scenario"].astype(str).eq(filters["fes_scenario"])
        & hourly["climate_member"].astype(str).eq(filters["climate_member"])
        & hourly["weather_year_role"].astype(str).eq(filters["weather_year_role"])
    )
    if include_case:
        mask &= hourly["smr_case"].astype(str).eq(filters["smr_case"])
    if year is not None:
        mask &= hourly["year"].astype(int).eq(int(year))
    return hourly.loc[mask].copy()


def hourly_impact_page(hourly: pd.DataFrame, filters: dict[str, str]) -> None:
    page_header(
        "Hourly system impact",
        "Inspect hourly residual demand, gas need, surplus, and true residual-load duration curves.",
    )
    year, start_date, end_date = select_year_and_range(hourly)

    subset = hourly_filter(hourly, filters, year=year, include_case=False)
    subset = subset[(subset["date"] >= start_date) & (subset["date"] <= end_date)].copy()
    if subset.empty:
        st.error("No hourly rows match the selected filters and date range.")
        return
    subset = add_context_labels(subset)

    st.subheader("Residual demand before and after SMRs")
    before = subset[subset["smr_case"].eq(BASE_CASE)].sort_values("timestamp_utc")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=before["timestamp_utc"],
            y=before["residual_before_smr_mw"],
            mode="lines",
            name="Before SMR",
            line=dict(width=2, color="#374151"),
        )
    )
    for case in [BASE_CASE, STRESS_CASE]:
        case_df = subset[subset["smr_case"].eq(case)].sort_values("timestamp_utc")
        if case_df.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=case_df["timestamp_utc"],
                y=case_df["residual_after_smr_mw"],
                mode="lines",
                name=f"After {short_case(case)}",
            )
        )
    fig.update_layout(
        xaxis_title="UTC time",
        yaxis_title="Residual demand (MW)",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(
            subset,
            x="timestamp_utc",
            y="gas_needed_after_mw",
            color="smr_case_label",
            labels={
                "timestamp_utc": "UTC time",
                "gas_needed_after_mw": "Gas needed after SMR (MW)",
                "smr_case_label": "SMR case",
            },
            title="Gas need after SMR",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.line(
            subset,
            x="timestamp_utc",
            y="surplus_after_smr_mw",
            color="smr_case_label",
            labels={
                "timestamp_utc": "UTC time",
                "surplus_after_smr_mw": "Surplus after SMR (MW)",
                "smr_case_label": "SMR case",
            },
            title="Surplus / oversupply proxy after SMR",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("True residual-load duration curve")
    st.caption(
        "This corrects the static PNG issue: the curve below sorts residual demand itself, not the SMR output reduction."
    )
    year_subset = hourly_filter(hourly, filters, year=year, include_case=False)
    fig = go.Figure()
    before_values = (
        year_subset[year_subset["smr_case"].eq(BASE_CASE)]["residual_before_smr_mw"]
        .sort_values(ascending=False)
        .reset_index(drop=True)
    )
    share = (before_values.index + 1) / len(before_values) * 100
    fig.add_trace(
        go.Scatter(
            x=share,
            y=before_values,
            mode="lines",
            name="Before SMR",
            line=dict(color="#374151", width=2),
        )
    )
    for case in [BASE_CASE, STRESS_CASE]:
        values = (
            year_subset[year_subset["smr_case"].eq(case)]["residual_after_smr_mw"]
            .sort_values(ascending=False)
            .reset_index(drop=True)
        )
        if values.empty:
            continue
        share = (values.index + 1) / len(values) * 100
        fig.add_trace(
            go.Scatter(x=share, y=values, mode="lines", name=f"After {short_case(case)}")
        )
    fig.update_layout(
        xaxis_title=f"Share of {year} hours (%)",
        yaxis_title="Residual demand (MW)",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Download selected hourly rows"):
        csv = subset.drop(columns=["date"], errors="ignore").to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV for selected range",
            data=csv,
            file_name=f"smr_hourly_subset_{year}_{start_date}_{end_date}.csv",
            mime="text/csv",
        )


def low_wind_page(hourly: pd.DataFrame, filters: dict[str, str]) -> None:
    page_header(
        "Low-wind resilience",
        "Explore the pressure-day cases where SMR output supports the system under low-wind supply conditions.",
    )
    rankings = load_low_wind_rankings()
    if rankings.empty:
        st.error("Low-wind ranking table is empty.")
        return

    st.subheader("Ranked low-wind pressure days")
    st.caption("Ranking is based on the additional gas-displacement proxy under the simultaneous stress-test versus the staggered base case.")
    ranking_display = rankings.head(50).copy()
    if "climate_member" in ranking_display.columns:
        ranking_display["demand_climate_scenario"] = ranking_display["climate_member"].map(label_climate_member)
    preferred_cols = [c for c in ["date", "fes_scenario", "demand_climate_scenario", "simultaneous_minus_staggered_mwh", BASE_CASE, STRESS_CASE] if c in ranking_display.columns]
    st.dataframe(ranking_display[preferred_cols] if preferred_cols else ranking_display, use_container_width=True, hide_index=True)

    choices = rankings.head(100).copy()
    choices["demand_climate_scenario"] = choices["climate_member"].map(label_climate_member)
    choices["choice_label"] = choices.apply(
        lambda r: f"{r['date']} | {r['fes_scenario']} | {r['demand_climate_scenario']} | Δ {r['simultaneous_minus_staggered_mwh']:,.0f} MWh",
        axis=1,
    )
    selected_label = st.selectbox("Select pressure day", choices["choice_label"].tolist())
    selected = choices.loc[choices["choice_label"].eq(selected_label)].iloc[0]

    date_selected = selected["date"]
    scenario_selected = str(selected["fes_scenario"])
    climate_selected = str(selected["climate_member"])

    day_rows = hourly[
        hourly["date"].eq(date_selected)
        & hourly["fes_scenario"].astype(str).eq(scenario_selected)
        & hourly["climate_member"].astype(str).eq(climate_selected)
        & hourly["weather_year_role"].astype(str).eq(LOW_WIND)
    ].copy()
    if day_rows.empty:
        st.error("No hourly rows found for the selected low-wind day.")
        return
    day_rows = add_context_labels(day_rows)

    kpi = day_rows.groupby("smr_case_label", observed=True).agg(
        smr_energy_mwh=("smr_total_delivered_mw", "sum"),
        gas_displacement_mwh=("gas_displacement_proxy_mw", "sum"),
        max_residual_after_mw=("residual_after_smr_mw", "max"),
        surplus_mwh=("surplus_after_smr_mw", "sum"),
    ).reset_index()
    st.subheader("Selected pressure-day metrics")
    st.dataframe(kpi, use_container_width=True, hide_index=True)

    fig = go.Figure()
    before = day_rows[day_rows["smr_case"].eq(BASE_CASE)].sort_values("timestamp_utc")
    fig.add_trace(
        go.Scatter(
            x=before["timestamp_utc"],
            y=before["residual_before_smr_mw"],
            mode="lines",
            name="Before SMR",
            line=dict(color="#374151", width=2),
        )
    )
    for case in [BASE_CASE, STRESS_CASE]:
        subset = day_rows[day_rows["smr_case"].eq(case)].sort_values("timestamp_utc")
        fig.add_trace(
            go.Scatter(
                x=subset["timestamp_utc"],
                y=subset["residual_after_smr_mw"],
                mode="lines",
                name=f"After {short_case(case)}",
            )
        )
    fig.update_layout(
        title=f"Low-wind pressure day: {date_selected}, {scenario_selected}, {label_climate_member(climate_selected)}",
        xaxis_title="UTC time",
        yaxis_title="Residual demand (MW)",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(
            day_rows,
            x="timestamp_utc",
            y="smr_total_delivered_mw",
            color="smr_case_label",
            labels={"timestamp_utc": "UTC time", "smr_total_delivered_mw": "SMR output (MW)"},
            title="SMR output during selected low-wind day",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.line(
            day_rows,
            x="timestamp_utc",
            y="gas_displacement_proxy_mw",
            color="smr_case_label",
            labels={"timestamp_utc": "UTC time", "gas_displacement_proxy_mw": "Gas displacement proxy (MW)"},
            title="Gas displacement proxy during selected low-wind day",
        )
        st.plotly_chart(fig, use_container_width=True)


def deployment_page(hourly: pd.DataFrame, annual: pd.DataFrame, filters: dict[str, str]) -> None:
    page_header(
        "SMR deployment assumptions",
        "Show how the three Wylfa SMR units enter the model and how the base case differs from the stress-test.",
    )
    assumptions = load_smr_assumptions()
    st.subheader("Unit assumptions")
    st.dataframe(assumptions, use_container_width=True, hide_index=True)

    if not assumptions.empty:
        total_nameplate = assumptions["nameplate_mwe"].sum()
        delivered_per_unit = assumptions["nameplate_mwe"].iloc[0] * assumptions["net_delivery_factor"].iloc[0]
        cols = st.columns(4)
        cols[0].metric("SMR units", fmt_int(len(assumptions)))
        cols[1].metric("Total nameplate capacity", fmt_mw(total_nameplate))
        cols[2].metric("Delivered output per available unit", fmt_mw(delivered_per_unit))
        cols[3].metric("Delivered fleet output when all available", fmt_mw(delivered_per_unit * len(assumptions)))
        if "forced_outage_rate" in assumptions.columns:
            cols[0].caption(f"Forced outage rate: {float(assumptions['forced_outage_rate'].iloc[0]):.1%}")
        if "planned_outage_window" in assumptions.columns:
            cols[1].caption(f"Planned outage window: {int(assumptions['planned_outage_window'].iloc[0])} days")

    st.subheader("Annual SMR output by unit")
    unit_subset = hourly_filter(hourly, filters, include_case=True)
    unit_energy = unit_subset.groupby("year", observed=True)[
        ["unit1_delivered_mw", "unit2_delivered_mw", "unit3_delivered_mw", "smr_total_delivered_mw"]
    ].sum().reset_index()
    for col in ["unit1_delivered_mw", "unit2_delivered_mw", "unit3_delivered_mw", "smr_total_delivered_mw"]:
        unit_energy[col.replace("_mw", "_twh")] = unit_energy[col] / 1_000_000
    unit_energy_long = unit_energy.melt(
        id_vars="year",
        value_vars=["unit1_delivered_twh", "unit2_delivered_twh", "unit3_delivered_twh"],
        var_name="unit",
        value_name="annual_energy_twh",
    )
    unit_energy_long["unit"] = unit_energy_long["unit"].str.replace("_delivered_twh", "", regex=False).str.replace("unit", "Unit ")
    fig = px.bar(
        unit_energy_long,
        x="year",
        y="annual_energy_twh",
        color="unit",
        labels={"year": "Year", "annual_energy_twh": "Annual delivered energy (TWh)", "unit": "SMR unit"},
        title=f"Unit-level delivered energy: {label_case(filters['smr_case'])}",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Scenario definitions")
    definitions = pd.DataFrame(
        [
            {
                "dimension": "Main model",
                "definition": "Average-wind supply case + staggered SMR commissioning",
                "interpretation": "Primary scenario used to communicate the modelled system impact.",
            },
            {
                "dimension": "SMR deployment stress-test",
                "definition": "All three SMR units online from 2035-01-01",
                "interpretation": "Robustness check showing the effect of faster availability, using the same unit capacity and outage assumptions.",
            },
            {
                "dimension": "Low-wind supply case",
                "definition": "Low-wind weather role and low-wind flag threshold in the hourly data",
                "interpretation": "Used to test how SMRs support the system during stressed renewable-output conditions.",
            },
        ]
    )
    st.dataframe(definitions, use_container_width=True, hide_index=True)


def clean_qa_checks(qa: pd.DataFrame) -> pd.DataFrame:
    name_map = {
        "base_model_case_coverage": "Base model case coverage",
        "dashboard_hourly_row_count": "Dashboard hourly row count",
        "duplicate_scenario_hour_keys": "Duplicate scenario-hour keys",
        "missing_values": "Missing values",
        "metric_recomputation_agreement": "Metric recomputation agreement",
        "supply_weather_case_coverage": "Supply/weather case coverage",
        "smr_case_coverage": "SMR case coverage",
        "annual_summary_coverage": "Annual summary coverage",
        "whole_period_summary_coverage": "Whole-period summary coverage",
        "chart_output_coverage": "Chart output coverage",
    }
    out = qa.copy()
    out["check"] = out["check_name"].map(name_map).fillna(out["check_name"].astype(str).str.replace("_", " ").str.title())
    out["interpretation"] = out.get("notes", "")
    return out[["check", "status", "expected", "observed", "interpretation"]]


def qa_methodology_page(annual: pd.DataFrame, period: pd.DataFrame) -> None:
    page_header(
        "Methodology and quality checks",
        "A concise explanation of the modelling logic, scenario dimensions, and data checks behind the dashboard.",
    )

    st.subheader("What the model is doing")
    st.markdown(
        """
        The dashboard shows a simplified GB system-impact model for three Wylfa SMRs.
        Demand, exogenous supply and imports are combined first; SMR output is then added; gas need and surplus are calculated from the resulting residual demand.

        The model is designed for scenario comparison and communication. It is not a wholesale-market dispatch model, a unit-commitment model, or a carbon-emissions model.
        """
    )

    st.subheader("Core balancing equations")
    st.code(
        """residual_before_smr_mw = demand_mw - exogenous_supply_mw - imports_net_baseline_mw
residual_after_smr_mw  = residual_before_smr_mw - smr_total_delivered_mw
gas_needed_before_mw   = max(residual_before_smr_mw, 0)
gas_needed_after_mw    = max(residual_after_smr_mw, 0)
gas_displacement_proxy = gas_needed_before_mw - gas_needed_after_mw
surplus_after_smr_mw   = max(-residual_after_smr_mw, 0)""",
        language="text",
    )

    st.subheader("Scenario dimensions")
    cols = st.columns(5)
    cols[0].metric("FES pathways", fmt_int(annual["fes_scenario"].nunique()))
    cols[1].metric("Demand climate scenarios", fmt_int(annual["climate_member"].nunique()))
    cols[2].metric("Supply/weather cases", fmt_int(annual["weather_year_role"].nunique()))
    cols[3].metric("SMR cases", fmt_int(annual["smr_case"].nunique()))
    cols[4].metric("Model years", f"{int(annual['year'].min())}–{int(annual['year'].max())}")

    with st.expander("Scenario label definitions", expanded=True):
        st.markdown("**Demand climate scenarios**")
        for member in sorted(annual["climate_member"].astype(str).unique()):
            st.markdown(f"- **{label_climate_member(member)}:** {CLIMATE_MEMBER_HELP.get(member, 'UKCP18-based demand climate realisation.')}")
        st.markdown("**Supply/weather cases**")
        for role in sorted(annual["weather_year_role"].astype(str).unique()):
            st.markdown(f"- **{label_weather_role(role)}:** {WEATHER_ROLE_DEFINITIONS.get(role, 'Supply-side weather sensitivity.')}")
        st.markdown("**SMR deployment cases**")
        st.markdown("- **Staggered commissioning:** phased deployment case used as the main model view.")
        st.markdown("- **Simultaneous commissioning stress-test:** all three units are treated as online from the first commissioning year to test faster availability.")

    st.subheader("Data quality checks")
    qa = load_qa_checks()
    cleaned = clean_qa_checks(qa)
    st.dataframe(cleaned, use_container_width=True, hide_index=True)
    if "status" in cleaned.columns:
        status_counts = cleaned["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        fig = px.bar(status_counts, x="status", y="count", title="Quality check status counts")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("How to interpret the stress-test")
    st.info(
        "The main result should be read from the staggered commissioning case. "
        "The simultaneous commissioning case is included to show how results change if all three units are available from 2035. "
        "It is useful for sensitivity analysis, but it should not be described as the central build-out assumption."
    )

def display_period_summary(period: pd.DataFrame) -> pd.DataFrame:
    df = add_context_labels(period)
    cols = [
        "fes_scenario",
        "demand_climate_scenario",
        "supply_weather_case",
        "smr_case_label",
        "cumulative_smr_energy_twh",
        "cumulative_gas_displacement_twh",
        "average_residual_demand_reduction_mw",
        "average_gas_displacement_proxy_mw",
        "total_surplus_hours",
        "total_low_wind_support_hours",
    ]
    return df[[c for c in cols if c in df.columns]].rename(
        columns={
            "fes_scenario": "FES pathway",
            "demand_climate_scenario": "Demand climate scenario",
            "supply_weather_case": "Supply/weather case",
            "smr_case_label": "SMR case",
            "cumulative_smr_energy_twh": "Cumulative SMR energy (TWh)",
            "cumulative_gas_displacement_twh": "Cumulative gas displacement proxy (TWh)",
            "average_residual_demand_reduction_mw": "Average residual reduction (MW)",
            "average_gas_displacement_proxy_mw": "Average gas displacement proxy (MW)",
            "total_surplus_hours": "Total surplus hours",
            "total_low_wind_support_hours": "Low-wind support hours",
        }
    )


def display_hourly_subset(subset: pd.DataFrame) -> pd.DataFrame:
    df = add_context_labels(subset)
    cols = [
        "timestamp_utc",
        "fes_scenario",
        "demand_climate_scenario",
        "supply_weather_case",
        "smr_case_label",
        "demand_mw",
        "wind_mw",
        "smr_total_delivered_mw",
        "residual_before_smr_mw",
        "residual_after_smr_mw",
        "gas_displacement_proxy_mw",
        "surplus_after_smr_mw",
    ]
    return df[[c for c in cols if c in df.columns]].rename(
        columns={
            "timestamp_utc": "Timestamp UTC",
            "fes_scenario": "FES pathway",
            "demand_climate_scenario": "Demand climate scenario",
            "supply_weather_case": "Supply/weather case",
            "smr_case_label": "SMR case",
            "demand_mw": "Demand (MW)",
            "wind_mw": "Wind output (MW)",
            "smr_total_delivered_mw": "SMR output (MW)",
            "residual_before_smr_mw": "Residual before SMR (MW)",
            "residual_after_smr_mw": "Residual after SMR (MW)",
            "gas_displacement_proxy_mw": "Gas displacement proxy (MW)",
            "surplus_after_smr_mw": "Surplus after SMR (MW)",
        }
    )


def data_explorer_page(hourly: pd.DataFrame, annual: pd.DataFrame, period: pd.DataFrame, filters: dict[str, str]) -> None:
    page_header(
        "Data explorer",
        "Inspect dashboard-ready tables and download filtered data extracts."
    )
    table = st.selectbox("Table", ["Period summary", "Annual summary", "Hourly subset"])
    if table == "Period summary":
        display = display_period_summary(period)
        st.dataframe(display, use_container_width=True, hide_index=True)
        st.download_button("Download period summary CSV", period.to_csv(index=False), "period_summary.csv", "text/csv")
    elif table == "Annual summary":
        subset = filter_annual(annual, filters, include_case=False)
        display = add_context_labels(subset)
        display_cols = [
            "year", "fes_scenario", "demand_climate_scenario", "supply_weather_case", "smr_case_label",
            "annual_smr_energy_twh", "annual_gas_displacement_twh", "average_residual_before_mw",
            "average_residual_after_mw", "surplus_hours_count", "low_wind_support_hours"
        ]
        st.dataframe(display[[c for c in display_cols if c in display.columns]], use_container_width=True, hide_index=True)
        st.download_button("Download selected annual summary CSV", subset.to_csv(index=False), "annual_summary_selected.csv", "text/csv")
    else:
        years = sorted(hourly["year"].dropna().astype(int).unique())
        year = st.selectbox("Year for extract", years, index=years.index(2036) if 2036 in years else 0)
        subset = hourly_filter(hourly, filters, year=year, include_case=False)
        display = display_hourly_subset(subset.head(2000))
        st.dataframe(display, use_container_width=True, hide_index=True)
        st.caption(f"Showing first 2,000 rows of {len(subset):,}. Use the download button for the full selected year/case comparison.")
        st.download_button(
            "Download selected hourly year CSV",
            subset.drop(columns=["date"], errors="ignore").to_csv(index=False),
            f"hourly_subset_{year}.csv",
            "text/csv",
        )

def main() -> None:
    require_access()
    require_dashboard_data()

    annual = load_annual()
    period = load_period()
    filters = sidebar_filters(annual)

    page = st.sidebar.radio(
        "Page",
        [
            "Overview",
            "Scenario explorer",
            "Hourly system impact",
            "Low-wind resilience",
            "SMR deployment assumptions",
            "Methodology and quality checks",
            "Data explorer",
        ],
    )

    # Load the hourly data only for pages that need it.
    hourly_pages = {
        "Hourly system impact",
        "Low-wind resilience",
        "SMR deployment assumptions",
        "Data explorer",
    }
    hourly = load_hourly() if page in hourly_pages else None

    if page == "Overview":
        overview_page(annual, period, filters)
    elif page == "Scenario explorer":
        scenario_explorer_page(annual, period, filters)
    elif page == "Hourly system impact" and hourly is not None:
        hourly_impact_page(hourly, filters)
    elif page == "Low-wind resilience" and hourly is not None:
        low_wind_page(hourly, filters)
    elif page == "SMR deployment assumptions" and hourly is not None:
        deployment_page(hourly, annual, filters)
    elif page == "Methodology and quality checks":
        qa_methodology_page(annual, period)
    elif page == "Data explorer" and hourly is not None:
        data_explorer_page(hourly, annual, period, filters)

    st.sidebar.divider()
    st.sidebar.caption("DATA70202 Applied Data Science group project. Not official Rolls-Royce SMR branding.")


if __name__ == "__main__":
    main()
