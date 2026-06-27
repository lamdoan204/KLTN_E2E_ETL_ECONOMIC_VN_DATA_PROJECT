"""
Dashboard Kinh tế Việt Nam
Kết nối Spark/Hive, đọc gold layer Delta tables, hiển thị theo từng fact.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pyspark.sql import SparkSession

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
APP_NAME             = "VN-Economy-Dashboard"
SPARK_MASTER_URL     = os.getenv("SPARK_MASTER_URL", "local[*]")
HIVE_METASTORE_URI   = os.getenv("HIVE_METASTORE_URI", "thrift://hive:9083")
MINIO_ENDPOINT       = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY     = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY     = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# ─────────────────────────────────────────
# STREAMLIT PAGE
# ─────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard Kinh tế Việt Nam",
    page_icon="🇻🇳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────
# CLASSIC STYLING
# ─────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Merriweather:wght@400;700&family=Source+Sans+3:wght@400;600&display=swap');

  html, body, [class*="css"] {
    font-family: 'Source Sans 3', sans-serif;
    background-color: #F7F5F0;
    color: #1E1E1E;
  }

  /* Sidebar */
  section[data-testid="stSidebar"] {
    background-color: #1C3557;
    color: #EAE6DC;
  }
  section[data-testid="stSidebar"] * {
    color: #EAE6DC !important;
  }
  section[data-testid="stSidebar"] .stSelectbox label,
  section[data-testid="stSidebar"] .stMultiSelect label,
  section[data-testid="stSidebar"] .stSlider label {
    color: #BFC9D4 !important;
    font-size: 0.82rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  /* Header banner */
  .dashboard-header {
    background: #1C3557;
    color: #F7F5F0;
    padding: 1.2rem 2rem;
    border-radius: 4px;
    margin-bottom: 1.5rem;
    display: flex;
    align-items: center;
    gap: 1rem;
  }
  .dashboard-header h1 {
    font-family: 'Merriweather', serif;
    font-size: 1.5rem;
    font-weight: 700;
    margin: 0;
    letter-spacing: -0.01em;
    color: #F7F5F0;
  }
  .dashboard-header p {
    margin: 0.2rem 0 0 0;
    font-size: 0.85rem;
    color: #9BBAD4;
  }

  /* KPI cards */
  .kpi-row { display: flex; gap: 1rem; margin-bottom: 1.5rem; }
  .kpi-card {
    background: #FFFFFF;
    border: 1px solid #DDD8CC;
    border-top: 3px solid #1C3557;
    border-radius: 3px;
    padding: 1rem 1.2rem;
    flex: 1;
  }
  .kpi-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #6B6455;
    margin-bottom: 0.3rem;
  }
  .kpi-value {
    font-family: 'Merriweather', serif;
    font-size: 1.6rem;
    font-weight: 700;
    color: #1C3557;
    line-height: 1;
  }
  .kpi-delta {
    font-size: 0.78rem;
    color: #3A7D44;
    margin-top: 0.25rem;
  }
  .kpi-delta.neg { color: #B03A2E; }

  /* Section titles */
  .section-title {
    font-family: 'Merriweather', serif;
    font-size: 1rem;
    font-weight: 700;
    color: #1C3557;
    border-bottom: 2px solid #C9B99A;
    padding-bottom: 0.3rem;
    margin: 1.5rem 0 0.8rem 0;
  }

  /* Tab styling */
  .stTabs [data-baseweb="tab-list"] {
    background-color: #FFFFFF;
    border-bottom: 2px solid #1C3557;
    gap: 0;
  }
  .stTabs [data-baseweb="tab"] {
    font-family: 'Source Sans 3', sans-serif;
    font-size: 0.82rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: #6B6455;
    padding: 0.6rem 1.1rem;
    border-radius: 0;
    border: none;
  }
  .stTabs [aria-selected="true"] {
    background-color: #1C3557 !important;
    color: #F7F5F0 !important;
  }

  /* Dataframe */
  .stDataFrame { border: 1px solid #DDD8CC; border-radius: 3px; }

  /* Divider */
  hr { border-color: #DDD8CC; }

  /* Chart containers */
  .chart-card {
    background: #FFFFFF;
    border: 1px solid #DDD8CC;
    border-radius: 3px;
    padding: 1rem;
    margin-bottom: 1rem;
  }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# SPARK SESSION (cached)
# ─────────────────────────────────────────
@st.cache_resource(show_spinner="Đang khởi tạo Spark Session…")
def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER_URL)
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.executor.cores", os.getenv("SPARK_EXECUTOR_CORES", "1"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


@st.cache_data(ttl=300, show_spinner=False)
def query(_spark, sql: str) -> pd.DataFrame:
    return _spark.sql(sql).toPandas()


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
CHART_COLORS = ["#1C3557", "#C9B99A", "#3A7D44", "#B03A2E", "#6B6455", "#9BBAD4"]

PLOTLY_LAYOUT = dict(
    font_family="Source Sans 3, sans-serif",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#FAFAF8",
    margin=dict(l=10, r=10, t=36, b=10),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    colorway=CHART_COLORS,
)

def fmt_num(v, unit=""):
    if pd.isna(v):
        return "—"
    if abs(v) >= 1_000:
        return f"{v:,.0f} {unit}".strip()
    return f"{v:,.2f} {unit}".strip()

def delta_html(v):
    if pd.isna(v):
        return ""
    cls = "neg" if v < 0 else ""
    sign = "▲" if v >= 0 else "▼"
    return f'<div class="kpi-delta {cls}">{sign} {abs(v):.2f}%</div>'

def kpi_card(label, value, delta=None, unit=""):
    dh = delta_html(delta) if delta is not None else ""
    st.markdown(
        f'<div class="kpi-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{fmt_num(value, unit)}</div>'
        f'{dh}</div>',
        unsafe_allow_html=True,
    )

def section(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)

def combo_chart(df, x, bars, lines, bar_labels=None, line_labels=None, title=""):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    bar_labels = bar_labels or bars
    line_labels = line_labels or lines
    for i, (col, lbl) in enumerate(zip(bars, bar_labels)):
        fig.add_trace(
            go.Bar(name=lbl, x=df[x], y=df[col], marker_color=CHART_COLORS[i], opacity=0.85),
            secondary_y=False,
        )
    for i, (col, lbl) in enumerate(zip(lines, line_labels)):
        fig.add_trace(
            go.Scatter(
                name=lbl, x=df[x], y=df[col],
                mode="lines+markers",
                line=dict(color=CHART_COLORS[len(bars) + i], width=2),
                marker=dict(size=5),
            ),
            secondary_y=True,
        )
    fig.update_layout(title_text=title, **PLOTLY_LAYOUT)
    return fig

def pie_chart(labels, values, title=""):
    fig = px.pie(names=labels, values=values, title=title,
                 color_discrete_sequence=CHART_COLORS, hole=0.35)
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig

def time_label(row):
    """Build readable time label from dim_time row."""
    if pd.notna(row.get("month")) and row.get("month", 0) > 0:
        return f"T{int(row['month'])}/{int(row['year'])}"
    if pd.notna(row.get("quarter")) and row.get("quarter", 0) > 0:
        return f"Q{int(row['quarter'])}/{int(row['year'])}"
    return str(int(row["year"]))


# ─────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────
def render_sidebar(spark):
    st.sidebar.markdown("## 🇻🇳 Bộ lọc toàn cục")
    dim_time = query(spark, "SELECT * FROM gold.dim_time ORDER BY year, quarter, month, day")

    years = sorted(dim_time["year"].dropna().unique().astype(int).tolist())
    sel_years = st.sidebar.multiselect("Năm", years, default=years[-4:] if len(years) >= 4 else years)

    quarters = sorted(dim_time["quarter"].dropna().unique().astype(int).tolist())
    sel_quarters = []
    if quarters:
        sel_quarters = st.sidebar.multiselect("Quý", quarters, default=quarters)

    months = sorted(dim_time["month"].dropna().unique().astype(int).tolist())
    sel_months = []
    if months:
        sel_months = st.sidebar.multiselect(
            "Tháng (áp dụng cho Trade)",
            months,
            default=months,
        )

    st.sidebar.markdown("---")
    st.sidebar.caption("Dữ liệu được truy vấn từ Gold Layer (Delta / Hive)")

    return sel_years, sel_quarters, sel_months, dim_time


def filter_time_keys(dim_time, years, quarters, months, granularity="quarter"):
    """Return list of time_key matching filter."""
    df = dim_time.copy()
    if years:
        df = df[df["year"].isin(years)]
    if granularity == "quarter" and quarters:
        df = df[df["quarter"].isin(quarters)]
    if granularity == "month" and months:
        df = df[df["month"].isin(months)]
    return df["time_key"].tolist()


# ─────────────────────────────────────────
# TAB 1 — GDP GROWTH
# ─────────────────────────────────────────
def tab_gdp(spark, dim_time, years, quarters):
    section("Tổng quan GDP")

    # Dimensions
    dim_sub = query(spark, "SELECT * FROM gold.dim_sub_sector")
    dim_sec = query(spark, "SELECT * FROM gold.dim_sector")

    # Sidebar sub-filters
    sectors = ["Tất cả"] + dim_sec["sector_name"].dropna().unique().tolist()
    sel_sector = st.selectbox("Ngành kinh tế", sectors, key="gdp_sector")

    sub_df = dim_sub.merge(dim_sec, on="sector_key", how="left")
    if sel_sector != "Tất cả":
        sub_df = sub_df[sub_df["sector_name"] == sel_sector]
    sub_keys = sub_df["sub_sector_key"].tolist()

    tkeys = filter_time_keys(dim_time, years, quarters, [], "quarter")
    if not tkeys or not sub_keys:
        st.info("Không có dữ liệu với bộ lọc hiện tại.")
        return

    tk_str = ",".join(map(str, tkeys))
    sk_str = ",".join(map(str, sub_keys))

    df = query(spark, f"""
        SELECT f.*, t.year, t.quarter
        FROM gold.fact_gdp_growth f
        JOIN gold.dim_time t ON f.time_key = t.time_key
        JOIN gold.dim_sub_sector ss ON f.sub_sector_key = ss.sub_sector_key
        JOIN gold.dim_sector s ON ss.sector_key = s.sector_key
        WHERE f.time_key IN ({tk_str}) AND f.sub_sector_key IN ({sk_str})
    """)
    df = df.merge(sub_df[["sub_sector_key", "sub_sector_name"]], on="sub_sector_key", how="left")
    if df.empty:
        st.info("Không có dữ liệu.")
        return

    df["time_label"] = df.apply(lambda r: f"Q{int(r['quarter'])}/{int(r['year'])}", axis=1)
    agg = df.groupby("time_label", sort=False).agg(
        market_value=("market_value", "sum"),
        constant_value=("constant_value", "sum"),
        market_yoy_growth_rate=("market_yoy_growth_rate", "mean"),
        real_yoy_growth_rate=("real_yoy_growth_rate", "mean"),
        gdp_share_pct=("gdp_share_pct", "sum"),
    ).reset_index()

    latest = agg.iloc[-1]

    # KPI row
    cols = st.columns(4)
    with cols[0]:
        kpi_card("Giá trị thị trường", latest["market_value"], latest["market_yoy_growth_rate"], df["unit"].iloc[0] if "unit" in df.columns else "")
    with cols[1]:
        kpi_card("Giá trị thực", latest["constant_value"], latest["real_yoy_growth_rate"])
    with cols[2]:
        kpi_card("Tăng trưởng YoY (danh nghĩa)", latest["market_yoy_growth_rate"], unit="%")
    with cols[3]:
        kpi_card("Tăng trưởng YoY (thực)", latest["real_yoy_growth_rate"], unit="%")

    # Combo chart: giá trị + tăng trưởng
    section("Giá trị GDP và tăng trưởng theo thời gian")
    fig = combo_chart(
        agg, "time_label",
        bars=["market_value", "constant_value"],
        lines=["market_yoy_growth_rate", "real_yoy_growth_rate"],
        bar_labels=["Giá trị danh nghĩa", "Giá trị thực"],
        line_labels=["Tăng trưởng danh nghĩa (%)", "Tăng trưởng thực (%)"],
        title="GDP theo giá hiện hành & tăng trưởng YoY",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Pie: cơ cấu ngành
    section("Cơ cấu GDP theo phân ngành")
    pie_df = df.groupby("sub_sector_name")["market_value"].sum().reset_index()
    fig_pie = pie_chart(pie_df["sub_sector_name"], pie_df["market_value"], "Tỷ trọng GDP theo phân ngành")
    st.plotly_chart(fig_pie, use_container_width=True)

    section("Dữ liệu chi tiết")
    display_cols = ["time_label", "sub_sector_name", "market_value", "constant_value",
                    "market_yoy_growth_rate", "real_yoy_growth_rate", "gdp_share_pct"]
    display_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(df[display_cols].rename(columns={
        "time_label": "Thời gian", "sub_sector_name": "Phân ngành",
        "market_value": "Giá trị TT", "constant_value": "Giá trị thực",
        "market_yoy_growth_rate": "% YoY TT", "real_yoy_growth_rate": "% YoY thực",
        "gdp_share_pct": "% GDP",
    }), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────
# TAB 2 — ĐẦU TƯ THEO NGÀNH
# ─────────────────────────────────────────
def tab_investment_sector(spark, dim_time, years, quarters):
    section("Tổng quan Đầu tư theo Ngành")

    dim_sec = query(spark, "SELECT * FROM gold.dim_sector")
    dim_sub = query(spark, "SELECT * FROM gold.dim_sub_sector")

    sectors = ["Tất cả"] + dim_sec["sector_name"].dropna().unique().tolist()
    sel_sector = st.selectbox("Ngành kinh tế", sectors, key="inv_sector")

    sub_df = dim_sub.merge(dim_sec, on="sector_key", how="left")
    if sel_sector != "Tất cả":
        filtered_sec = dim_sec[dim_sec["sector_name"] == sel_sector]
        sub_df = sub_df[sub_df["sector_key"].isin(filtered_sec["sector_key"])]

    tkeys = filter_time_keys(dim_time, years, quarters, [], "quarter")
    if not tkeys:
        st.info("Không có dữ liệu với bộ lọc hiện tại.")
        return

    tk_str = ",".join(map(str, tkeys))
    df = query(spark, f"""
        SELECT f.*, t.year, t.quarter, s.sector_name, ss.sub_sector_name
        FROM gold.fact_investment_by_sector f
        JOIN gold.dim_time t ON f.time_key = t.time_key
        LEFT JOIN gold.dim_sector s ON f.sector_key = s.sector_key
        LEFT JOIN gold.dim_sub_sector ss ON f.sub_sector_key = ss.sub_sector_key
        WHERE f.time_key IN ({tk_str})
    """)
    if sel_sector != "Tất cả":
        df = df[df["sector_name"] == sel_sector]
    if df.empty:
        st.info("Không có dữ liệu.")
        return

    df["time_label"] = df.apply(lambda r: f"Q{int(r['quarter'])}/{int(r['year'])}", axis=1)
    agg = df.groupby("time_label", sort=False).agg(
        investment_value=("investment_value", "sum"),
        yoy_growth_rate=("yoy_growth_rate", "mean"),
    ).reset_index()

    latest = agg.iloc[-1]
    cols = st.columns(3)
    with cols[0]:
        kpi_card("Giá trị đầu tư", latest["investment_value"], latest["yoy_growth_rate"])
    with cols[1]:
        kpi_card("Tăng trưởng YoY", latest["yoy_growth_rate"], unit="%")
    with cols[2]:
        avg_share = df["all_sector_share_pct"].mean() if "all_sector_share_pct" in df.columns else None
        kpi_card("Tỷ trọng trung bình toàn nền", avg_share, unit="%")

    section("Giá trị đầu tư và tăng trưởng")
    fig = combo_chart(
        agg, "time_label",
        bars=["investment_value"], lines=["yoy_growth_rate"],
        bar_labels=["Giá trị đầu tư"], line_labels=["Tăng trưởng YoY (%)"],
        title="Đầu tư theo ngành qua các quý",
    )
    st.plotly_chart(fig, use_container_width=True)

    section("Cơ cấu đầu tư theo ngành")
    pie_df = df.groupby("sector_name")["investment_value"].sum().reset_index().dropna()
    if not pie_df.empty:
        st.plotly_chart(pie_chart(pie_df["sector_name"], pie_df["investment_value"], "Tỷ trọng đầu tư theo ngành"), use_container_width=True)

    section("Top phân ngành đầu tư")
    top_sub = df.groupby("sub_sector_name")["investment_value"].sum().nlargest(10).reset_index()
    fig_bar = px.bar(
        top_sub, x="investment_value", y="sub_sector_name", orientation="h",
        title="Top 10 phân ngành theo giá trị đầu tư",
        color_discrete_sequence=[CHART_COLORS[0]],
    )
    fig_bar.update_layout(**PLOTLY_LAYOUT)
    st.plotly_chart(fig_bar, use_container_width=True)

    section("Dữ liệu chi tiết")
    st.dataframe(df[["time_label", "sector_name", "sub_sector_name",
                      "investment_value", "yoy_growth_rate", "sector_share_pct", "all_sector_share_pct"]].rename(columns={
        "time_label": "Thời gian", "sector_name": "Ngành", "sub_sector_name": "Phân ngành",
        "investment_value": "Giá trị ĐT", "yoy_growth_rate": "% YoY",
        "sector_share_pct": "% Ngành", "all_sector_share_pct": "% Toàn nền",
    }), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────
# TAB 3 — SẢN LƯỢNG NÔNG NGHIỆP
# ─────────────────────────────────────────
def tab_crop(spark, dim_time, years, quarters):
    section("Tổng quan Sản lượng Cây trồng")

    dim_crop = query(spark, "SELECT * FROM gold.dim_crop")
    categories = ["Tất cả"] + dim_crop["crop_category"].dropna().unique().tolist()
    sel_cat = st.selectbox("Nhóm cây trồng", categories, key="crop_cat")

    crops = dim_crop.copy()
    if sel_cat != "Tất cả":
        crops = crops[crops["crop_category"] == sel_cat]
    sel_crops = st.multiselect(
        "Cây trồng cụ thể",
        crops["crop_name"].tolist(),
        default=crops["crop_name"].tolist()[:5],
        key="crop_names",
    )

    tkeys = filter_time_keys(dim_time, years, quarters, [], "quarter")
    if not tkeys:
        st.info("Không có dữ liệu với bộ lọc hiện tại.")
        return

    tk_str = ",".join(map(str, tkeys))
    df = query(spark, f"""
        SELECT f.*, t.year, t.quarter, c.crop_name, c.crop_category
        FROM gold.fact_crop_yield f
        JOIN gold.dim_time t ON f.time_key = t.time_key
        JOIN gold.dim_crop c ON f.crop_key = c.crop_key
        WHERE f.time_key IN ({tk_str})
    """)
    if sel_cat != "Tất cả":
        df = df[df["crop_category"] == sel_cat]
    if sel_crops:
        df = df[df["crop_name"].isin(sel_crops)]
    if df.empty:
        st.info("Không có dữ liệu.")
        return

    df["time_label"] = df.apply(lambda r: f"Q{int(r['quarter'])}/{int(r['year'])}" if pd.notna(r.get("quarter")) and r["quarter"] > 0 else str(int(r["year"])), axis=1)

    latest = df.sort_values(["year", "quarter"], na_position="last").groupby("crop_name").last().reset_index()
    cols = st.columns(3)
    with cols[0]:
        kpi_card("Tổng sản lượng", df["yield_value"].sum())
    with cols[1]:
        kpi_card("Diện tích canh tác", df["area"].sum())
    with cols[2]:
        kpi_card("Năng suất TB", df["productivity"].mean())

    section("Sản lượng và tăng trưởng YoY")
    agg = df.groupby("time_label", sort=False).agg(
        yield_value=("yield_value", "sum"),
        area=("area", "sum"),
        yield_yoy_growth_rate=("yield_yoy_growth_rate", "mean"),
    ).reset_index()
    fig = combo_chart(
        agg, "time_label",
        bars=["yield_value", "area"], lines=["yield_yoy_growth_rate"],
        bar_labels=["Sản lượng", "Diện tích"], line_labels=["Tăng trưởng YoY (%)"],
        title="Sản lượng và diện tích canh tác",
    )
    st.plotly_chart(fig, use_container_width=True)

    section("Cơ cấu sản lượng theo nhóm cây trồng")
    pie_df = df.groupby("crop_category")["yield_value"].sum().reset_index().dropna()
    if not pie_df.empty:
        st.plotly_chart(pie_chart(pie_df["crop_category"], pie_df["yield_value"], "Tỷ trọng theo nhóm cây"), use_container_width=True)

    section("Top cây trồng theo sản lượng")
    top_crop = df.groupby("crop_name")["yield_value"].sum().nlargest(10).reset_index()
    fig_bar = px.bar(top_crop, x="yield_value", y="crop_name", orientation="h",
                     color_discrete_sequence=[CHART_COLORS[2]])
    fig_bar.update_layout(title_text="Top 10 cây trồng", **PLOTLY_LAYOUT)
    st.plotly_chart(fig_bar, use_container_width=True)

    section("Dữ liệu chi tiết")
    st.dataframe(df[["time_label", "crop_name", "crop_category", "area",
                      "yield_value", "productivity", "yield_yoy_growth_rate", "yield_share_pct"]].rename(columns={
        "time_label": "Thời gian", "crop_name": "Cây trồng", "crop_category": "Nhóm",
        "area": "Diện tích", "yield_value": "Sản lượng", "productivity": "Năng suất",
        "yield_yoy_growth_rate": "% YoY", "yield_share_pct": "% Tổng",
    }), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────
# TAB 4 — SẢN LƯỢNG CÔNG NGHIỆP
# ─────────────────────────────────────────
def tab_production(spark, dim_time, years, quarters):
    section("Tổng quan Sản lượng Công nghiệp")

    dim_prod = query(spark, "SELECT * FROM gold.dim_product")
    categories = ["Tất cả"] + dim_prod["product_category"].dropna().unique().tolist()
    sel_cat = st.selectbox("Nhóm sản phẩm", categories, key="prod_cat")
    types = ["Tất cả"] + dim_prod["product_type"].dropna().unique().tolist()
    sel_type = st.selectbox("Loại sản phẩm", types, key="prod_type")

    tkeys = filter_time_keys(dim_time, years, quarters, [], "quarter")
    if not tkeys:
        st.info("Không có dữ liệu với bộ lọc hiện tại.")
        return

    tk_str = ",".join(map(str, tkeys))
    df = query(spark, f"""
        SELECT f.*, t.year, t.quarter, p.product_name, p.product_type, p.product_category
        FROM gold.fact_production_output f
        JOIN gold.dim_time t ON f.time_key = t.time_key
        JOIN gold.dim_product p ON f.product_key = p.product_key
        WHERE f.time_key IN ({tk_str})
    """)
    if sel_cat != "Tất cả":
        df = df[df["product_category"] == sel_cat]
    if sel_type != "Tất cả":
        df = df[df["product_type"] == sel_type]
    if df.empty:
        st.info("Không có dữ liệu.")
        return

    df["time_label"] = df.apply(lambda r: f"Q{int(r['quarter'])}/{int(r['year'])}", axis=1)

    agg = df.groupby("time_label", sort=False).agg(
        value=("value", "sum"),
        yoy_growth_rate=("yoy_growth_rate", "mean"),
        qoq_growth_rate=("qoq_growth_rate", "mean"),
    ).reset_index()
    latest = agg.iloc[-1]

    cols = st.columns(3)
    with cols[0]:
        kpi_card("Tổng sản lượng", latest["value"])
    with cols[1]:
        kpi_card("Tăng trưởng YoY", latest["yoy_growth_rate"], unit="%")
    with cols[2]:
        kpi_card("Tăng trưởng QoQ", latest["qoq_growth_rate"], unit="%")

    section("Sản lượng và tăng trưởng theo quý")
    fig = combo_chart(
        agg, "time_label",
        bars=["value"], lines=["yoy_growth_rate", "qoq_growth_rate"],
        bar_labels=["Giá trị sản lượng"], line_labels=["% YoY", "% QoQ"],
        title="Sản lượng và tốc độ tăng trưởng",
    )
    st.plotly_chart(fig, use_container_width=True)

    section("Cơ cấu theo nhóm sản phẩm")
    pie_df = df.groupby("product_category")["value"].sum().reset_index().dropna()
    if not pie_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(pie_chart(pie_df["product_category"], pie_df["value"], "Tỷ trọng theo nhóm"), use_container_width=True)
        with c2:
            top10 = df.groupby("product_name")["value"].sum().nlargest(10).reset_index()
            fig_bar = px.bar(top10, x="value", y="product_name", orientation="h",
                             color_discrete_sequence=[CHART_COLORS[0]])
            fig_bar.update_layout(title_text="Top 10 sản phẩm", **PLOTLY_LAYOUT)
            st.plotly_chart(fig_bar, use_container_width=True)

    section("Dữ liệu chi tiết")
    st.dataframe(df[["time_label", "product_name", "product_type", "product_category",
                      "value", "yoy_growth_rate", "qoq_growth_rate", "product_share_pct"]].rename(columns={
        "time_label": "Thời gian", "product_name": "Sản phẩm", "product_type": "Loại",
        "product_category": "Nhóm", "value": "Giá trị", "yoy_growth_rate": "% YoY",
        "qoq_growth_rate": "% QoQ", "product_share_pct": "% Tổng",
    }), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────
# TAB 5 — THƯƠNG MẠI QUỐC TẾ
# ─────────────────────────────────────────
def tab_trade(spark, dim_time, years, months):
    section("Tổng quan Thương mại Quốc tế")

    dim_prod = query(spark, "SELECT * FROM gold.dim_product")
    categories = ["Tất cả"] + dim_prod["product_category"].dropna().unique().tolist()
    sel_cat = st.selectbox("Nhóm hàng hóa", categories, key="trade_cat")

    tkeys = filter_time_keys(dim_time, years, [], months, "month")
    if not tkeys:
        st.info("Không có dữ liệu với bộ lọc hiện tại.")
        return

    tk_str = ",".join(map(str, tkeys))
    df = query(spark, f"""
        SELECT f.*, t.year, t.month, p.product_name, p.product_type, p.product_category
        FROM gold.fact_international_trade f
        JOIN gold.dim_time t ON f.time_key = t.time_key
        JOIN gold.dim_product p ON f.product_key = p.product_key
        WHERE f.time_key IN ({tk_str})
    """)
    if sel_cat != "Tất cả":
        df = df[df["product_category"] == sel_cat]
    if df.empty:
        st.info("Không có dữ liệu.")
        return

    df["time_label"] = df.apply(lambda r: f"T{int(r['month'])}/{int(r['year'])}", axis=1)

    agg = df.groupby("time_label", sort=False).agg(
        trade_value=("trade_value", "sum"),
        mom_growth_rate=("mom_growth_rate", "mean"),
        yoy_growth_rate=("yoy_growth_rate", "mean"),
    ).reset_index()
    latest = agg.iloc[-1]

    cols = st.columns(3)
    with cols[0]:
        kpi_card("Giá trị XNK", latest["trade_value"], latest["yoy_growth_rate"])
    with cols[1]:
        kpi_card("Tăng trưởng MoM", latest["mom_growth_rate"], unit="%")
    with cols[2]:
        kpi_card("Tăng trưởng YoY", latest["yoy_growth_rate"], unit="%")

    section("Giá trị và tăng trưởng theo tháng")
    fig = combo_chart(
        agg, "time_label",
        bars=["trade_value"], lines=["mom_growth_rate", "yoy_growth_rate"],
        bar_labels=["Giá trị XNK"], line_labels=["% MoM", "% YoY"],
        title="Thương mại quốc tế theo tháng",
    )
    st.plotly_chart(fig, use_container_width=True)

    section("Cơ cấu hàng hóa XNK")
    c1, c2 = st.columns(2)
    pie_val = df.groupby("product_category")["trade_value"].sum().reset_index().dropna()
    with c1:
        if not pie_val.empty:
            st.plotly_chart(pie_chart(pie_val["product_category"], pie_val["trade_value"], "Tỷ trọng theo nhóm hàng"), use_container_width=True)
    with c2:
        top10 = df.groupby("product_name")["trade_value"].sum().nlargest(10).reset_index()
        fig_bar = px.bar(top10, x="trade_value", y="product_name", orientation="h",
                         color_discrete_sequence=[CHART_COLORS[3]])
        fig_bar.update_layout(title_text="Top 10 mặt hàng", **PLOTLY_LAYOUT)
        st.plotly_chart(fig_bar, use_container_width=True)

    section("Dữ liệu chi tiết")
    st.dataframe(df[["time_label", "product_name", "product_category",
                      "trade_value", "quantity", "mom_growth_rate", "yoy_growth_rate", "product_share_pct"]].rename(columns={
        "time_label": "Thời gian", "product_name": "Hàng hóa", "product_category": "Nhóm",
        "trade_value": "Giá trị", "quantity": "Khối lượng",
        "mom_growth_rate": "% MoM", "yoy_growth_rate": "% YoY", "product_share_pct": "% Tổng",
    }), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────
# TAB 6 — ĐẦU TƯ XÃ HỘI
# ─────────────────────────────────────────
def tab_social_investment(spark, dim_time, years, quarters):
    section("Tổng quan Đầu tư Toàn xã hội")

    dim_cap = query(spark, "SELECT * FROM gold.dim_capital_source")
    sources = ["Tất cả"] + dim_cap["source_name"].dropna().unique().tolist()
    sel_source = st.selectbox("Nguồn vốn", sources, key="si_source")

    tkeys = filter_time_keys(dim_time, years, quarters, [], "quarter")
    if not tkeys:
        st.info("Không có dữ liệu với bộ lọc hiện tại.")
        return

    tk_str = ",".join(map(str, tkeys))
    df = query(spark, f"""
        SELECT f.*, t.year, t.quarter, cs.source_name
        FROM gold.fact_social_total_investment f
        JOIN gold.dim_time t ON f.time_key = t.time_key
        JOIN gold.dim_capital_source cs ON f.capital_source_key = cs.capital_source_key
        WHERE f.time_key IN ({tk_str})
    """)
    if sel_source != "Tất cả":
        df = df[df["source_name"] == sel_source]
    if df.empty:
        st.info("Không có dữ liệu.")
        return

    df["time_label"] = df.apply(lambda r: f"Q{int(r['quarter'])}/{int(r['year'])}", axis=1)
    agg = df.groupby("time_label", sort=False).agg(
        investment_value=("investment_value", "sum"),
        qoq_growth_rate=("qoq_growth_rate", "mean"),
        yoy_growth_rate=("yoy_growth_rate", "mean"),
    ).reset_index()
    latest = agg.iloc[-1]

    cols = st.columns(3)
    with cols[0]:
        kpi_card("Tổng vốn đầu tư XH", latest["investment_value"], latest["yoy_growth_rate"])
    with cols[1]:
        kpi_card("Tăng trưởng QoQ", latest["qoq_growth_rate"], unit="%")
    with cols[2]:
        kpi_card("Tăng trưởng YoY", latest["yoy_growth_rate"], unit="%")

    section("Vốn đầu tư và tăng trưởng theo quý")
    fig = combo_chart(
        agg, "time_label",
        bars=["investment_value"], lines=["qoq_growth_rate", "yoy_growth_rate"],
        bar_labels=["Vốn đầu tư"], line_labels=["% QoQ", "% YoY"],
        title="Đầu tư toàn xã hội qua các quý",
    )
    st.plotly_chart(fig, use_container_width=True)

    section("Cơ cấu theo nguồn vốn")
    pie_df = df.groupby("source_name")["investment_value"].sum().reset_index().dropna()
    if not pie_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(pie_chart(pie_df["source_name"], pie_df["investment_value"], "Tỷ trọng theo nguồn vốn"), use_container_width=True)
        with c2:
            fig_line = px.line(
                agg, x="time_label", y=["qoq_growth_rate", "yoy_growth_rate"],
                title="Tốc độ tăng trưởng (%)",
                labels={"value": "%", "variable": "Chỉ tiêu"},
                color_discrete_sequence=CHART_COLORS[1:],
            )
            fig_line.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig_line, use_container_width=True)

    section("Dữ liệu chi tiết")
    st.dataframe(df[["time_label", "source_name", "investment_value",
                      "qoq_growth_rate", "yoy_growth_rate", "source_share_pct"]].rename(columns={
        "time_label": "Thời gian", "source_name": "Nguồn vốn",
        "investment_value": "Giá trị ĐT", "qoq_growth_rate": "% QoQ",
        "yoy_growth_rate": "% YoY", "source_share_pct": "% Tổng",
    }), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    # Header
    st.markdown("""
    <div class="dashboard-header">
      <div>
        <h1>🇻🇳 Dashboard Kinh tế Việt Nam</h1>
        <p>Dữ liệu từ Gold Layer · Delta Lake trên MinIO · Truy vấn qua Apache Spark / Hive Metastore</p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Spark
    try:
        spark = get_spark_session()
    except Exception as e:
        st.error(f"Không thể kết nối Spark: {e}")
        st.stop()

    # Sidebar + global filters
    sel_years, sel_quarters, sel_months, dim_time = render_sidebar(spark)

    # Tabs
    tabs = st.tabs([
        "📈 Tăng trưởng GDP",
        "🏗️ Đầu tư theo Ngành",
        "🌾 Nông nghiệp",
        "🏭 Sản lượng CN",
        "🚢 Thương mại QT",
        "💰 Đầu tư Xã hội",
    ])

    with tabs[0]:
        tab_gdp(spark, dim_time, sel_years, sel_quarters)
    with tabs[1]:
        tab_investment_sector(spark, dim_time, sel_years, sel_quarters)
    with tabs[2]:
        tab_crop(spark, dim_time, sel_years, sel_quarters)
    with tabs[3]:
        tab_production(spark, dim_time, sel_years, sel_quarters)
    with tabs[4]:
        tab_trade(spark, dim_time, sel_years, sel_months)
    with tabs[5]:
        tab_social_investment(spark, dim_time, sel_years, sel_quarters)


if __name__ == "__main__":
    main()
