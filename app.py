"""
app.py  —  NYC Taxi Analytics Explorer
DuckDB Internals Dashboard  |  DSCI 551 Course Project  |  Chenyu Zuo

Run:
    streamlit run app.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import textwrap
import time
import os

from queries import QUERIES, QUERY_MAP, CATEGORIES

# ── DB Connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    """Create in-memory DuckDB and load data from Hugging Face parquet."""
    con = duckdb.connect()  # 纯内存数据库，不需要文件
    
    with st.spinner("⏳ Loading data (~150MB, first load only)..."):
        con.execute("""
            CREATE TABLE taxi_trips AS
            SELECT * FROM read_parquet(
                'https://huggingface.co/datasets/Shyanne257/nyc-taxi-duckdb/resolve/main/taxi.duckdb'
            )
        """)
        con.execute("""
            CREATE TABLE taxi_zones AS
            SELECT * FROM read_parquet(
                'https://huggingface.co/datasets/Shyanne257/nyc-taxi-duckdb/resolve/main/taxi.duckdb'
            )
        """)
    return con

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Taxi · DuckDB Internals Explorer",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* Dark header bar */
.main-header {
    background: linear-gradient(135deg, #0f1117 0%, #1a1f2e 50%, #0d1117 100%);
    border-bottom: 2px solid #f5c518;
    padding: 1.5rem 2rem;
    margin: -1rem -1rem 2rem -1rem;
    border-radius: 0 0 8px 8px;
}
.main-header h1 {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.6rem;
    color: #f5c518;
    margin: 0;
    letter-spacing: -0.5px;
}
.main-header p {
    color: #8892a4;
    margin: 0.3rem 0 0 0;
    font-size: 0.85rem;
}

/* Metric cards */
.metric-row {
    display: flex; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap;
}
.metric-card {
    background: #1a1f2e;
    border: 1px solid #2a3040;
    border-radius: 8px;
    padding: 1rem 1.4rem;
    flex: 1; min-width: 140px;
}
.metric-card .label {
    font-size: 0.72rem; color: #8892a4;
    text-transform: uppercase; letter-spacing: 0.8px;
    margin-bottom: 0.3rem;
}
.metric-card .value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.4rem; color: #f5c518; font-weight: 600;
}
.metric-card .sub {
    font-size: 0.75rem; color: #6b7585; margin-top: 0.2rem;
}

/* Internals box */
.internals-box {
    background: #0d1117;
    border-left: 3px solid #f5c518;
    border-radius: 0 6px 6px 0;
    padding: 1rem 1.2rem;
    margin: 0.5rem 0;
    font-size: 0.88rem;
    line-height: 1.6;
    color: #c8d0dc;
}
.internals-box .box-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #f5c518;
    margin-bottom: 0.5rem;
}

/* EXPLAIN box */
.explain-box {
    background: #0a0e17;
    border: 1px solid #2a3040;
    border-radius: 6px;
    padding: 1rem;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
    color: #7dd3fc;
    overflow-x: auto;
    white-space: pre;
    line-height: 1.5;
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 2px solid #2a3040;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    padding: 0.5rem 1.2rem;
    color: #8892a4;
}
.stTabs [aria-selected="true"] {
    color: #f5c518 !important;
    border-bottom: 2px solid #f5c518 !important;
}

/* Sidebar */
.sidebar-section {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: #f5c518;
    margin: 1rem 0 0.4rem 0;
}

/* Timing badge */
.timing-badge {
    display: inline-block;
    background: #1a2a1a;
    border: 1px solid #2d5a2d;
    color: #4caf50;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
    padding: 0.2rem 0.7rem;
    border-radius: 20px;
    margin-bottom: 1rem;
}

/* Query category pill */
.category-pill {
    display: inline-block;
    background: #1a2035;
    border: 1px solid #2a3a55;
    color: #7db3f5;
    font-size: 0.75rem;
    padding: 0.15rem 0.6rem;
    border-radius: 20px;
    margin-bottom: 0.5rem;
    font-family: 'IBM Plex Mono', monospace;
}
</style>
""", unsafe_allow_html=True)


# ── DB Connection (cached) ────────────────────────────────────────────────────
DB_PATH = "taxi.duckdb"

@st.cache_resource
def get_connection():
    if not os.path.exists(DB_PATH):
        st.error(
            f"Database file '{DB_PATH}' not found. "
            "Please run `python setup_db.py` first."
        )
        st.stop()
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data(ttl=3600)
def run_query(sql: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(sql).df()


@st.cache_data(ttl=3600)
def get_explain(sql: str, analyze: bool = False) -> str:
    con = get_connection()
    keyword = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    try:
        result = con.execute(f"{keyword}\n{sql}").df()
        # DuckDB returns explain in column 'explain_value' or second column
        col = result.columns[-1]
        return result[col].iloc[0] if len(result) > 0 else "No output"
    except Exception as e:
        return f"Error running {keyword}: {e}"


@st.cache_data(ttl=3600)
def get_db_stats():
    con = get_connection()
    n_trips = con.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
    n_zones = con.execute("SELECT COUNT(*) FROM taxi_zones").fetchone()[0]
    months  = con.execute(
        "SELECT MIN(pickup_date), MAX(pickup_date) FROM taxi_trips"
    ).fetchone()
    total_rev = con.execute(
        "SELECT SUM(total_amount) FROM taxi_trips"
    ).fetchone()[0]
    return {
        "n_trips":   n_trips,
        "n_zones":   n_zones,
        "date_min":  months[0],
        "date_max":  months[1],
        "total_rev": total_rev,
    }


# ── Chart renderer ────────────────────────────────────────────────────────────
DUCK_YELLOW = "#f5c518"
PLOTLY_TEMPLATE = "plotly_dark"

def render_chart(df: pd.DataFrame, q: dict):
    ctype = q.get("chart_type", "bar")
    x, y  = q.get("x_col"), q.get("y_col")

    if ctype == "bar":
        fig = px.bar(
            df, x=x, y=y,
            color_discrete_sequence=[DUCK_YELLOW],
            template=PLOTLY_TEMPLATE,
        )
        fig.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font_color="#c8d0dc",
            xaxis=dict(tickfont=dict(size=11)),
            yaxis=dict(gridcolor="#1e2535"),
            margin=dict(l=0, r=0, t=30, b=0),
            height=380,
        )
        fig.update_traces(marker_line_width=0)

    elif ctype == "line":
        fig = px.line(
            df, x=x, y=y,
            markers=True,
            color_discrete_sequence=[DUCK_YELLOW],
            template=PLOTLY_TEMPLATE,
        )
        fig.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font_color="#c8d0dc",
            yaxis=dict(gridcolor="#1e2535"),
            margin=dict(l=0, r=0, t=30, b=0),
            height=380,
        )

    elif ctype == "scatter":
        color_col = df.columns[2] if len(df.columns) > 2 else None
        fig = px.scatter(
            df.head(500), x=x, y=y,
            color=color_col,
            opacity=0.6,
            color_continuous_scale="YlOrRd",
            template=PLOTLY_TEMPLATE,
        )
        fig.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font_color="#c8d0dc",
            yaxis=dict(gridcolor="#1e2535"),
            xaxis=dict(gridcolor="#1e2535"),
            margin=dict(l=0, r=0, t=30, b=0),
            height=380,
        )

    else:
        st.dataframe(df, use_container_width=True)
        return

    st.plotly_chart(fig, use_container_width=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🚕 DuckDB Explorer")
    st.markdown("*DSCI 551 · Chenyu Zuo*")
    st.divider()

    st.markdown('<div class="sidebar-section">Select Category</div>', unsafe_allow_html=True)
    selected_category = st.selectbox(
        "Category", CATEGORIES, label_visibility="collapsed"
    )

    category_queries = [q for q in QUERIES if q["category"] == selected_category]
    st.markdown('<div class="sidebar-section">Select Query</div>', unsafe_allow_html=True)
    query_titles = [q["title"] for q in category_queries]
    selected_title = st.selectbox(
        "Query", query_titles, label_visibility="collapsed"
    )
    selected_query = next(q for q in category_queries if q["title"] == selected_title)

    st.divider()
    st.markdown('<div class="sidebar-section">EXPLAIN Options</div>', unsafe_allow_html=True)
    show_explain         = st.checkbox("Show EXPLAIN (logical plan)", value=True)
    show_explain_analyze = st.checkbox("Show EXPLAIN ANALYZE (with timing)", value=False)
    st.caption("⚠️ EXPLAIN ANALYZE re-executes the query and may take a few seconds.")

    st.divider()
    st.markdown('<div class="sidebar-section">About</div>', unsafe_allow_html=True)
    st.caption(
        "This dashboard demonstrates DuckDB's vectorized execution engine "
        "on ~7M NYC Yellow Taxi trips (Jan–Feb 2026). "
        "Each query includes a detailed mapping between application behavior "
        "and DuckDB's internal execution."
    )


# ── Main header ───────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
  <h1>🚕 NYC Taxi · DuckDB Internals Explorer</h1>
  <p>DSCI 551 Course Project · Vectorized Execution on 7M Yellow Taxi Trips · Chenyu Zuo</p>
</div>
""", unsafe_allow_html=True)


# ── DB Stats row ──────────────────────────────────────────────────────────────
stats = get_db_stats()
st.markdown(f"""
<div class="metric-row">
  <div class="metric-card">
    <div class="label">Total Trips</div>
    <div class="value">{stats['n_trips']:,}</div>
    <div class="sub">Jan + Feb 2026</div>
  </div>
  <div class="metric-card">
    <div class="label">Total Revenue</div>
    <div class="value">${stats['total_rev']/1e6:.1f}M</div>
    <div class="sub">All trips combined</div>
  </div>
  <div class="metric-card">
    <div class="label">Taxi Zones</div>
    <div class="value">{stats['n_zones']}</div>
    <div class="sub">NYC zone lookup</div>
  </div>
  <div class="metric-card">
    <div class="label">Date Range</div>
    <div class="value" style="font-size:1rem">{str(stats['date_min'])[:10]} → {str(stats['date_max'])[:10]}</div>
    <div class="sub">2 months of data</div>
  </div>
  <div class="metric-card">
    <div class="label">Database</div>
    <div class="value" style="font-size:1rem">DuckDB</div>
    <div class="sub">Embedded OLAP · In-process</div>
  </div>
</div>
""", unsafe_allow_html=True)

st.divider()

# ── Query header ──────────────────────────────────────────────────────────────
st.markdown(
    f'<span class="category-pill">{selected_query["category"]}</span>',
    unsafe_allow_html=True,
)
st.markdown(f"## {selected_query['title']}")

# ── Run query & measure time ──────────────────────────────────────────────────
t0 = time.perf_counter()
df_result = run_query(selected_query["sql"])
elapsed = time.perf_counter() - t0

st.markdown(
    f'<div class="timing-badge">⚡ Query executed in {elapsed*1000:.0f} ms '
    f'· {len(df_result):,} rows returned</div>',
    unsafe_allow_html=True,
)

# ── Main tabs ─────────────────────────────────────────────────────────────────
tab_chart, tab_data, tab_internals, tab_sql = st.tabs([
    "📊 Chart", "📋 Results Table", "🔬 DB Internals", "🗒️ SQL & EXPLAIN"
])

with tab_chart:
    render_chart(df_result, selected_query)

with tab_data:
    st.dataframe(df_result, use_container_width=True, height=400)
    st.caption(f"{len(df_result):,} rows · {len(df_result.columns)} columns")

with tab_internals:
    col1, col2 = st.columns([1, 1], gap="large")

    with col1:
        st.markdown("#### 🖥️ What the Application Does")
        st.markdown(
            f'<div class="internals-box">'
            f'<div class="box-title">Application Behavior</div>'
            f'{selected_query["description"]}'
            f'</div>',
            unsafe_allow_html=True,
        )

        st.markdown("#### ⚙️ What DuckDB Does Internally")
        st.markdown(
            f'<div class="internals-box">'
            f'<div class="box-title">Internal Execution</div>'
            f'{selected_query["internals"]}'
            f'</div>',
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown("#### 💡 Why This Matters")
        st.markdown(
            f'<div class="internals-box" style="border-left-color:#4caf50;">'
            f'<div class="box-title" style="color:#4caf50;">Performance Insight</div>'
            f'{selected_query["why_matters"]}'
            f'</div>',
            unsafe_allow_html=True,
        )

        st.markdown("#### 📐 Query Metrics")
        st.markdown(f"""
<div class="internals-box" style="border-left-color:#7db3f5;">
<div class="box-title" style="color:#7db3f5;">Runtime Stats</div>
<b>Rows scanned:</b> ~{stats['n_trips']:,} (full table)<br>
<b>Rows returned:</b> {len(df_result):,}<br>
<b>Columns in table:</b> 20<br>
<b>Execution time:</b> {elapsed*1000:.0f} ms (cached after first run)<br>
<b>DuckDB chunk size:</b> 2,048 rows/vector<br>
<b>Estimated chunks:</b> ~{stats['n_trips']//2048:,}
</div>
""", unsafe_allow_html=True)

with tab_sql:
    st.markdown("#### SQL Query")
    st.code(selected_query["sql"].strip(), language="sql")

    if show_explain:
        st.markdown("#### EXPLAIN (Logical Query Plan)")
        with st.spinner("Generating plan..."):
            plan = get_explain(selected_query["sql"], analyze=False)
        st.markdown(
            f'<div class="explain-box">{plan}</div>',
            unsafe_allow_html=True,
        )

    if show_explain_analyze:
        st.markdown("#### EXPLAIN ANALYZE (Physical Plan + Timing)")
        st.warning(
            "EXPLAIN ANALYZE re-executes the full query. "
            "This may take several seconds for 7M rows."
        )
        with st.spinner("Running EXPLAIN ANALYZE..."):
            t_ea = time.perf_counter()
            plan_analyze = get_explain(selected_query["sql"], analyze=True)
            t_ea = time.perf_counter() - t_ea
        st.caption(f"EXPLAIN ANALYZE completed in {t_ea:.2f}s")
        st.markdown(
            f'<div class="explain-box">{plan_analyze}</div>',
            unsafe_allow_html=True,
        )

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "DuckDB Internals Explorer · DSCI 551 Spring 2026 · Chenyu Zuo · USC ID 2933-8178-16  "
    "| Data: NYC TLC Yellow Taxi Jan–Feb 2026 · Zone lookup: NYC TLC Open Data"
)
