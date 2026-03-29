"""
dashboard.py
------------
Streamlit BI dashboard for the US COVID-19 SQLite database.

Run:
    streamlit run dashboard.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from load import get_connection, run_query, DEFAULT_DB

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="US COVID-19 Analytics",
    page_icon="🦠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #1e2130;
        border-radius: 10px;
        padding: 16px 20px;
        border-left: 4px solid #4e8cff;
    }
    h1 { color: #e0e6ff; }
    .stSidebar { background: #161b2e; }
</style>
""", unsafe_allow_html=True)


# ── DB connection (cached) ────────────────────────────────────────────────────
@st.cache_resource
def get_conn(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    if not p.exists():
        st.error(f"Database not found: {p}\nPlease run `python pipeline.py` first.")
        st.stop()
    return get_connection(p)


# ── Data loaders (cached) ─────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_national_cases(_conn) -> pd.DataFrame:
    return run_query(_conn, "national_cases_trend")

@st.cache_data(ttl=3600)
def load_national_vax(_conn) -> pd.DataFrame:
    return run_query(_conn, "national_vax_trend")

@st.cache_data(ttl=3600)
def load_top_states(_conn) -> pd.DataFrame:
    return run_query(_conn, "top_states_cases")

@st.cache_data(ttl=3600)
def load_vax_by_state(_conn) -> pd.DataFrame:
    return run_query(_conn, "latest_vax_by_state")

@st.cache_data(ttl=3600)
def load_state_scatter(_conn) -> pd.DataFrame:
    return run_query(_conn, "state_cases_vs_vax")

@st.cache_data(ttl=3600)
def load_state_series(_conn, state: str) -> pd.DataFrame:
    sql = """
        SELECT report_date, new_cases, new_cases_7d_avg,
               new_deaths, new_deaths_7d_avg
        FROM fact_cases
        WHERE state_code = ?
        ORDER BY report_date
    """
    return pd.read_sql_query(sql, _conn, params=(state,), parse_dates=["report_date"])


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Controls")
    db_path = st.text_input("Database path", str(DEFAULT_DB))
    conn = get_conn(db_path)

    # date range filter
    min_d_row = conn.execute("SELECT MIN(report_date) FROM fact_cases").fetchone()
    max_d_row = conn.execute("SELECT MAX(report_date) FROM fact_cases").fetchone()
    min_date  = pd.to_datetime(min_d_row[0])
    max_date  = pd.to_datetime(max_d_row[0])

    date_range = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    start_date, end_date = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])

    st.markdown("---")
    # State selector for drill-down
    states = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT state_code FROM fact_cases ORDER BY state_code"
        )
    ]
    selected_state = st.selectbox("State drill-down", states, index=states.index("CA") if "CA" in states else 0)


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🦠 US COVID-19 Analytics Dashboard")
st.caption(f"Data range: **{start_date.date()}** → **{end_date.date()}**  |  Source: CDC")


# ── KPI Row ───────────────────────────────────────────────────────────────────
kpi = conn.execute("""
    SELECT
        SUM(new_cases)   AS total_new_cases,
        SUM(new_deaths)  AS total_new_deaths,
        (SELECT MAX(doses_administered)  FROM fact_vax_national) AS total_doses,
        (SELECT MAX(fully_vaccinated)    FROM fact_vax_national) AS total_fully_vax
    FROM fact_cases
    WHERE state_code != 'US'
""").fetchone()

col1, col2, col3, col4 = st.columns(4)
def fmt(v): return f"{v:,.0f}" if v else "N/A"

col1.metric("Total Cases Reported",     fmt(kpi[0]))
col2.metric("Total Deaths Reported",    fmt(kpi[1]))
col3.metric("Doses Administered",       fmt(kpi[2]))
col4.metric("Fully Vaccinated",         fmt(kpi[3]))

st.markdown("---")


# ── Tab layout ────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 National Trends",
    "💉 Vaccination Progress",
    "🗺️ State Comparison",
    "🔍 State Drill-Down",
])


# ────────────────────────── TAB 1: National Trends ───────────────────────────
with tab1:
    df_cases = load_national_cases(conn)
    df_cases = df_cases[(df_cases["report_date"] >= start_date) &
                        (df_cases["report_date"] <= end_date)]

    st.subheader("National Daily New Cases")
    if not df_cases.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_cases["report_date"], y=df_cases["new_cases"],
            name="Daily new cases", marker_color="rgba(78,140,255,0.4)",
        ))
        if "new_cases_7d_avg" in df_cases.columns:
            fig.add_trace(go.Scatter(
                x=df_cases["report_date"], y=df_cases["new_cases_7d_avg"],
                name="7-day avg", line=dict(color="#ff6b6b", width=2),
            ))
        fig.update_layout(
            xaxis_title="Date", yaxis_title="Cases",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No case data available for the selected date range.")


# ───────────────────────── TAB 2: Vaccination Progress ───────────────────────
with tab2:
    df_vax = load_national_vax(conn)
    df_vax = df_vax[(df_vax["report_date"] >= start_date) &
                    (df_vax["report_date"] <= end_date)]

    st.subheader("National Vaccination Progress")
    if not df_vax.empty:
        # Cumulative chart
        fig_cum = go.Figure()
        for col, label, color in [
            ("doses_administered", "Doses Administered", "#4e8cff"),
            ("fully_vaccinated",   "Fully Vaccinated",   "#00d4aa"),
            ("boosters",           "Boosters",           "#ff9f43"),
        ]:
            if col in df_vax.columns:
                fig_cum.add_trace(go.Scatter(
                    x=df_vax["report_date"], y=df_vax[col],
                    name=label, mode="lines",
                    line=dict(color=color, width=2),
                    fill="tozeroy" if col == "doses_administered" else None,
                    fillcolor="rgba(78,140,255,0.08)" if col == "doses_administered" else None,
                ))
        fig_cum.update_layout(
            title="Cumulative Vaccination Counts",
            xaxis_title="Date", yaxis_title="People",
            height=380,
        )
        st.plotly_chart(fig_cum, use_container_width=True)

        # Daily doses bar
        if "daily_doses_7d_avg" in df_vax.columns:
            fig_daily = px.bar(
                df_vax, x="report_date", y="daily_doses",
                title="Daily Doses Administered (with 7-day avg)",
                color_discrete_sequence=["rgba(0,212,170,0.4)"],
                labels={"daily_doses": "Daily Doses", "report_date": "Date"},
                height=320,
            )
            fig_daily.add_scatter(
                x=df_vax["report_date"], y=df_vax["daily_doses_7d_avg"],
                mode="lines", name="7-day avg",
                line=dict(color="#ff6b6b", width=2),
            )
            st.plotly_chart(fig_daily, use_container_width=True)
    else:
        st.info("No vaccination data for the selected range.")


# ──────────────────────────── TAB 3: State Comparison ────────────────────────
with tab3:
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Top 15 States – Total Cases")
        df_top = load_top_states(conn)
        if not df_top.empty:
            fig_bar = px.bar(
                df_top.sort_values("total_cases"),
                x="total_cases", y="state_code",
                orientation="h",
                color="total_cases",
                color_continuous_scale="Blues",
                labels={"total_cases": "Total Cases", "state_code": "State"},
                height=480,
            )
            fig_bar.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_bar, use_container_width=True)

    with col_b:
        st.subheader("Vaccination Rate by State (latest)")
        df_vax_state = load_vax_by_state(conn)
        if not df_vax_state.empty:
            fig_vax = px.bar(
                df_vax_state.sort_values("pct_fully_vaccinated", ascending=False).head(30),
                x="state_code", y="pct_fully_vaccinated",
                color="pct_fully_vaccinated",
                color_continuous_scale="Teal",
                labels={"pct_fully_vaccinated": "% Fully Vaccinated", "state_code": "State"},
                height=480,
            )
            fig_vax.add_hline(y=70, line_dash="dash", line_color="white",
                              annotation_text="70 % threshold")
            fig_vax.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_vax, use_container_width=True)

    st.subheader("Cases per 100k vs Vaccination Rate (by State)")
    df_scatter = load_state_scatter(conn)
    if not df_scatter.empty:
        fig_s = px.scatter(
            df_scatter.dropna(),
            x="pct_fully_vaccinated",
            y="total_cases_per_100k",
            size="total_deaths_per_100k",
            text="state_code",
            color="total_deaths_per_100k",
            color_continuous_scale="RdYlGn_r",
            labels={
                "pct_fully_vaccinated": "% Fully Vaccinated",
                "total_cases_per_100k": "Cases per 100k",
                "total_deaths_per_100k": "Deaths per 100k",
            },
            title="Higher vaccination ↔ fewer cases per 100k  (bubble size = deaths per 100k)",
            height=480,
        )
        fig_s.update_traces(textposition="top center", marker=dict(sizemin=4))
        st.plotly_chart(fig_s, use_container_width=True)


# ─────────────────────────── TAB 4: State Drill-Down ─────────────────────────
with tab4:
    st.subheader(f"State Drill-Down: **{selected_state}**")
    df_state = load_state_series(conn, selected_state)
    df_state = df_state[(df_state["report_date"] >= start_date) &
                        (df_state["report_date"] <= end_date)]

    if df_state.empty:
        st.info(f"No data for {selected_state} in the selected range.")
    else:
        fig_d = go.Figure()
        fig_d.add_trace(go.Bar(
            x=df_state["report_date"], y=df_state["new_cases"],
            name="Daily Cases", marker_color="rgba(78,140,255,0.4)",
        ))
        if "new_cases_7d_avg" in df_state.columns:
            fig_d.add_trace(go.Scatter(
                x=df_state["report_date"], y=df_state["new_cases_7d_avg"],
                name="Cases 7-day avg", line=dict(color="#4e8cff", width=2),
            ))
        fig_d.add_trace(go.Bar(
            x=df_state["report_date"], y=df_state["new_deaths"],
            name="Daily Deaths", marker_color="rgba(255,99,99,0.4)",
            yaxis="y2",
        ))
        fig_d.update_layout(
            title=f"{selected_state} – Daily Cases & Deaths",
            yaxis=dict(title="Cases"),
            yaxis2=dict(title="Deaths", overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=450,
        )
        st.plotly_chart(fig_d, use_container_width=True)

        # Summary stats
        st.markdown("**Summary statistics**")
        summary = df_state[["new_cases","new_deaths"]].describe().round(1)
        st.dataframe(summary, use_container_width=True)


# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("Data sourced from CDC Socrata APIs  •  Pipeline: extract → transform → SQLite → Streamlit")
