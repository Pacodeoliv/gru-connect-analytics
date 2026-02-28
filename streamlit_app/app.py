import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="GRU Connect Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Paths — inside Docker the data volume is mounted at /app/data
# ---------------------------------------------------------------------------
GOLD_DIR = Path("/app/data/gold")

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* KPI cards */
    .kpi-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #2a2a4a;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        text-align: center;
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0;
        line-height: 1.2;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #8892b0;
        margin: 0;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .critical  { color: #FF6B6B; }
    .medium    { color: #FECA57; }
    .safe      { color: #00D2D3; }
    .neutral   { color: #CCD6F6; }

    /* Risk badges in tables */
    .risk-critical {
        background: #FF6B6B22; color: #FF6B6B;
        padding: 2px 10px; border-radius: 12px; font-weight: 600;
    }
    .risk-medium {
        background: #FECA5722; color: #FECA57;
        padding: 2px 10px; border-radius: 12px; font-weight: 600;
    }
    .risk-safe {
        background: #00D2D322; color: #00D2D3;
        padding: 2px 10px; border-radius: 12px; font-weight: 600;
    }

    /* Sidebar */
    [data-testid="stSidebar"] { background: #0a0a1a; }

    /* Remove Streamlit branding */
    #MainMenu { visibility: hidden; }
    footer    { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Risk color map (consistent everywhere)
RISK_COLORS = {
    "Risco Crítico": "#FF6B6B",
    "Risco Médio": "#FECA57",
    "Seguro": "#00D2D3",
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_gold_data():
    """Load parquet files exported by export_gold.py."""
    data = {}
    files = {
        "connections": GOLD_DIR / "fato_conexoes.parquet",
        "airports": GOLD_DIR / "dim_aeroportos.parquet",
        "airlines": GOLD_DIR / "dim_empresas.parquet",
        "calendar": GOLD_DIR / "dim_calendario.parquet",
    }
    for key, path in files.items():
        if path.exists():
            data[key] = pd.read_parquet(path)
        else:
            data[key] = pd.DataFrame()
    return data


def has_data(data: dict) -> bool:
    return not data["connections"].empty


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------
def kpi_card(label: str, value, css_class: str = "neutral"):
    st.markdown(
        f"""
        <div class="kpi-card">
            <p class="kpi-value {css_class}">{value}</p>
            <p class="kpi-label">{label}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def no_data_message():
    st.markdown("## ✈️ Welcome to GRU Connect Analytics")
    st.info(
        "**No data available yet.** Run the pipeline first to see results.\n\n"
        "```bash\n"
        "# 1. Start the stack\n"
        "make up\n\n"
        "# 2. Open Airflow UI and trigger the Bronze DAG\n"
        "#    http://localhost:8080  (admin / admin)\n"
        "#    Unpause and trigger: dag_bronze_ingestion\n\n"
        "# 3. The Silver and Gold DAGs run automatically.\n"
        "#    After Gold completes, the export step populates this dashboard.\n"
        "```"
    )


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
def page_overview(data: dict):
    df = data["connections"]
    airlines = data["airlines"]

    st.markdown("## Dashboard Overview")
    st.caption("Passenger connection risk analysis at Guarulhos International Airport (GRU/SBGR)")

    # --- KPIs ---
    total = len(df)
    critical_count = len(df[df["desc_status_risco"] == "Risco Crítico"])
    critical_pct = f"{critical_count / total * 100:.1f}%" if total > 0 else "0%"
    avg_window = f"{df['janela_conexao_min'].mean():.0f} min" if total > 0 else "—"

    # Top risk airline
    if not airlines.empty and total > 0:
        top_airline_code = (
            df[df["desc_status_risco"] == "Risco Crítico"]
            .groupby("cd_icao_empresa")
            .size()
            .sort_values(ascending=False)
        )
        if not top_airline_code.empty:
            code = top_airline_code.index[0]
            match = airlines[airlines["cd_icao_empresa"] == code]
            top_airline = match["nm_empresa"].iloc[0] if not match.empty else code
        else:
            top_airline = "—"
    else:
        top_airline = "—"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi_card("Total Connections", f"{total:,}")
    with c2:
        kpi_card("Critical Risk", critical_pct, "critical")
    with c3:
        kpi_card("Avg Window", avg_window, "safe")
    with c4:
        kpi_card("Top Risk Airline", top_airline, "medium")

    st.markdown("---")

    # --- Charts ---
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### Risk Distribution")
        risk_counts = (
            df["desc_status_risco"]
            .value_counts()
            .reset_index()
            .rename(columns={"index": "Risk", "desc_status_risco": "Risk", "count": "Count"})
        )
        if "Risk" not in risk_counts.columns:
            risk_counts.columns = ["Risk", "Count"]
        fig = px.pie(
            risk_counts,
            values="Count",
            names="Risk",
            color="Risk",
            color_discrete_map=RISK_COLORS,
            hole=0.5,
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#CCD6F6",
            showlegend=True,
            legend=dict(orientation="h", y=-0.1),
            margin=dict(t=20, b=40, l=20, r=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown("#### Connection Window Distribution")
        fig2 = px.histogram(
            df,
            x="janela_conexao_min",
            nbins=30,
            color="desc_status_risco",
            color_discrete_map=RISK_COLORS,
            labels={"janela_conexao_min": "Connection Window (min)", "desc_status_risco": "Risk"},
        )
        fig2.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#CCD6F6",
            bargap=0.1,
            margin=dict(t=20, b=40, l=20, r=20),
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # --- Airlines bar chart ---
    st.markdown("#### Critical Connections by Airline")
    critical_by_airline = (
        df[df["desc_status_risco"] == "Risco Crítico"]
        .groupby("cd_icao_empresa")
        .size()
        .reset_index(name="critical_count")
        .sort_values("critical_count", ascending=True)
        .tail(10)
    )
    if not airlines.empty:
        critical_by_airline = critical_by_airline.merge(
            airlines[["cd_icao_empresa", "nm_empresa"]], on="cd_icao_empresa", how="left"
        )
        critical_by_airline["label"] = critical_by_airline["nm_empresa"].fillna(
            critical_by_airline["cd_icao_empresa"]
        )
    else:
        critical_by_airline["label"] = critical_by_airline["cd_icao_empresa"]

    fig3 = px.bar(
        critical_by_airline,
        x="critical_count",
        y="label",
        orientation="h",
        color_discrete_sequence=["#FF6B6B"],
        labels={"critical_count": "Critical Connections", "label": ""},
    )
    fig3.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#CCD6F6",
        margin=dict(t=20, b=20, l=20, r=20),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig3, use_container_width=True)


def page_flight_search(data: dict):
    df = data["connections"]
    airlines = data["airlines"]

    st.markdown("## Flight Search")
    st.caption("Search connections by flight number or airline code")

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        search_flight = st.text_input(
            "Flight Number",
            placeholder="e.g. 1234",
            help="Search by arrival or departure flight number",
        )
    with col2:
        airline_options = ["All"] + sorted(df["cd_icao_empresa"].dropna().unique().tolist())
        selected_airline = st.selectbox("Airline (ICAO)", airline_options)
    with col3:
        risk_options = ["All"] + list(RISK_COLORS.keys())
        selected_risk = st.selectbox("Risk Level", risk_options)

    # Apply filters
    filtered = df.copy()
    if search_flight:
        filtered = filtered[
            (filtered["nr_voo_chegada"].astype(str).str.contains(search_flight, case=False, na=False))
            | (filtered["nr_voo_partida"].astype(str).str.contains(search_flight, case=False, na=False))
        ]
    if selected_airline != "All":
        filtered = filtered[filtered["cd_icao_empresa"] == selected_airline]
    if selected_risk != "All":
        filtered = filtered[filtered["desc_status_risco"] == selected_risk]

    # Results
    st.markdown(f"**{len(filtered):,}** connections found")

    if not filtered.empty:
        display_cols = [
            "cd_icao_empresa",
            "nr_voo_chegada",
            "nr_voo_partida",
            "janela_conexao_min",
            "desc_status_risco",
            "dt_chegada_real",
            "dt_partida_prevista",
        ]
        existing_cols = [c for c in display_cols if c in filtered.columns]
        show_df = filtered[existing_cols].sort_values("janela_conexao_min").head(200)
        show_df = show_df.rename(
            columns={
                "cd_icao_empresa": "Airline",
                "nr_voo_chegada": "Arrival Flight",
                "nr_voo_partida": "Departure Flight",
                "janela_conexao_min": "Window (min)",
                "desc_status_risco": "Risk",
                "dt_chegada_real": "Actual Arrival",
                "dt_partida_prevista": "Planned Departure",
            }
        )
        st.dataframe(
            show_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Window (min)": st.column_config.NumberColumn(format="%.0f min"),
                "Risk": st.column_config.TextColumn(),
            },
        )
    else:
        st.warning("No connections match your search criteria.")


def page_airlines(data: dict):
    df = data["connections"]
    airlines = data["airlines"]

    st.markdown("## Airlines Analysis")
    st.caption("Risk breakdown by airline operating at GRU")

    # Build summary table
    summary = (
        df.groupby(["cd_icao_empresa", "desc_status_risco"])
        .size()
        .reset_index(name="count")
    )
    pivot = summary.pivot_table(
        index="cd_icao_empresa", columns="desc_status_risco", values="count", fill_value=0
    ).reset_index()

    for risk in RISK_COLORS:
        if risk not in pivot.columns:
            pivot[risk] = 0

    pivot["Total"] = pivot[list(RISK_COLORS.keys())].sum(axis=1)
    pivot["Critical %"] = (pivot.get("Risco Crítico", 0) / pivot["Total"] * 100).round(1)
    pivot = pivot.sort_values("Total", ascending=False)

    # Merge airline names
    if not airlines.empty:
        pivot = pivot.merge(
            airlines[["cd_icao_empresa", "nm_empresa"]], on="cd_icao_empresa", how="left"
        )
        pivot["Airline"] = pivot["nm_empresa"].fillna(pivot["cd_icao_empresa"])
    else:
        pivot["Airline"] = pivot["cd_icao_empresa"]

    display = pivot[
        ["Airline", "cd_icao_empresa", "Risco Crítico", "Risco Médio", "Seguro", "Total", "Critical %"]
    ].rename(columns={
        "cd_icao_empresa": "ICAO",
        "Risco Crítico": "Critical",
        "Risco Médio": "Medium",
        "Seguro": "Safe",
    })

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Critical %": st.column_config.ProgressColumn(
                format="%.1f%%", min_value=0, max_value=100
            ),
        },
    )

    # Stacked bar chart
    st.markdown("#### Risk Composition by Airline (Top 15)")
    top15 = pivot.head(15)
    fig = go.Figure()
    for risk, color in RISK_COLORS.items():
        if risk in top15.columns:
            fig.add_trace(
                go.Bar(name=risk, x=top15["Airline"], y=top15[risk], marker_color=color)
            )
    fig.update_layout(
        barmode="stack",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#CCD6F6",
        margin=dict(t=20, b=40, l=20, r=20),
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Sidebar & routing
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("# ✈️ GRU Connect")
    st.markdown("*Connection Risk Analytics*")
    st.markdown("---")
    page = st.radio(
        "Navigate",
        ["Overview", "Flight Search", "Airlines"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown(
        """
        <div style="font-size: 0.75rem; color: #5a5a7a;">
        Data source: ANAC VRA<br>
        Airport: GRU / SBGR<br>
        Stack: PySpark · dbt · Airflow
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
data = load_gold_data()

if not has_data(data):
    no_data_message()
else:
    if page == "Overview":
        page_overview(data)
    elif page == "Flight Search":
        page_flight_search(data)
    elif page == "Airlines":
        page_airlines(data)
