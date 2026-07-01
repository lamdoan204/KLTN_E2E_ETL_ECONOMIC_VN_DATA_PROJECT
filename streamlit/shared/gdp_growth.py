"""
GDP Growth Performance Dashboard.

Module này chứa toàn bộ logic của dashboard GDP Growth:
    - Truy vấn dữ liệu từ Spark (Gold layer).
    - Join các bảng dimension.
    - Áp dụng bộ lọc (filter) toàn cục.
    - Tính toán KPI.
    - Vẽ toàn bộ biểu đồ bằng Plotly.
    - Inject CSS để tạo giao diện chuyên nghiệp, đồng bộ tone màu
      với dashboard Sales Performance.

app.py và tabs/gdp.py không được chứa logic xử lý dữ liệu;
toàn bộ nằm trong file này, được expose qua hàm `render_dashboard()`.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from shared.spark import get_spark_session

# ====================================================================
# THEME / COLOR CONSTANTS
# ====================================================================

COLOR_BACKGROUND = "#081A36"
COLOR_CARD = "#102B55"
COLOR_BORDER = "#2C6FB8"
COLOR_HEADER = "#1B4F9C"
COLOR_ACCENT = "#3FA9F5"
COLOR_POSITIVE = "#2ECC71"
COLOR_NEGATIVE = "#E74C3C"
COLOR_TEXT = "#EAF2FB"
COLOR_TEXT_MUTED = "#9FB7D8"

# Bảng màu rời rạc dùng cho các biểu đồ phân loại theo Sector
DISCRETE_PALETTE = [
    "#3FA9F5",
    "#2ECC71",
    "#E74C3C",
    "#F5B041",
    "#9B59B6",
    "#1ABC9C",
    "#E67E22",
    "#5DADE2",
    "#F1948A",
    "#48C9B0",
]

CHART_PLOT_BG = "#090A44"
CHART_PAPER_BG = "#090A44"
CHART_FONT_COLOR = "#1A1A1A"


# ====================================================================
# CSS INJECTION
# ====================================================================

def inject_custom_css() -> None:
    """Inject CSS tuỳ chỉnh cho toàn bộ dashboard GDP Growth.

    Tạo giao diện đồng bộ tone màu tối (dark navy), card bo góc, có
    shadow nhẹ, border xanh dương, đồng nhất với dashboard Sales
    Performance.
    """
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .gdp-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .gdp-header h1 {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .gdp-header p {{
            color: #E7F1FE;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .gdp-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 14px;
        }}

        .gdp-section-title {{
            color: {COLOR_TEXT};
            font-size: 18px;
            font-weight: 600;
            margin: 6px 0 12px 4px;
            border-left: 4px solid {COLOR_ACCENT};
            padding-left: 10px;
        }}

        .kpi-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 16px 14px;
            text-align: center;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            height: 100%;
        }}

        .kpi-label {{
            color: {COLOR_TEXT_MUTED};
            font-size: 13px;
            font-weight: 500;
            margin-bottom: 6px;
            text-transform: uppercase;
            letter-spacing: 0.4px;
        }}

        .kpi-value {{
            color: {COLOR_TEXT};
            font-size: 24px;
            font-weight: 700;
        }}

        .kpi-positive {{
            color: {COLOR_POSITIVE};
        }}

        .kpi-negative {{
            color: {COLOR_NEGATIVE};
        }}

        .gdp-chart-wrapper {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 10px 14px 4px 14px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 16px;
        }}

        section[data-testid="stSidebar"] {{
            background-color: {COLOR_CARD};
            border-right: 1px solid {COLOR_BORDER};
        }}

        div[data-baseweb="select"] > div {{
            background-color: {COLOR_CARD};
            border-color: {COLOR_BORDER};
            color: {COLOR_TEXT};
        }}

        .stTabs [data-baseweb="tab-list"] {{
            gap: 6px;
        }}

        .stTabs [data-baseweb="tab"] {{
            background-color: {COLOR_CARD};
            border-radius: 10px 10px 0 0;
            color: {COLOR_TEXT_MUTED};
            border: 1px solid {COLOR_BORDER};
        }}

        .stTabs [aria-selected="true"] {{
            background-color: {COLOR_HEADER};
            color: #FFFFFF;
        }}

        .empty-state {{
            color: {COLOR_TEXT_MUTED};
            text-align: center;
            padding: 40px 0;
            font-size: 15px;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    """Render header chính của dashboard."""
    st.markdown(
        """
        <div class="gdp-header">
            <h1>GDP Growth Performance Dashboard</h1>
            <p>Theo dõi tăng trưởng GDP theo Sector / Sub-sector / Quarter</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ====================================================================
# DATA LOADING (SPARK)
# ====================================================================

@st.cache_data(show_spinner="Đang tải dữ liệu GDP Growth...")
def load_data() -> pd.DataFrame:
    """Truy vấn dữ liệu GDP Growth từ Gold layer bằng Spark.

    Thực hiện join giữa fact_gdp_growth với dim_time, dim_sub_sector,
    dim_sector. Dữ liệu chỉ được convert sang Pandas ở bước cuối cùng
    (sau khi đã join xong bằng Spark), phục vụ cho việc filter/vẽ
    biểu đồ phía Streamlit.

    Returns:
        pd.DataFrame: Dữ liệu GDP Growth đã join đầy đủ dimension.
    """
    spark = get_spark_session()

    fact: SparkDataFrame = spark.table("gold.fact_gdp_growth")
    dim_time: SparkDataFrame = spark.table("gold.dim_time")
    dim_sub_sector: SparkDataFrame = spark.table("gold.dim_sub_sector")
    dim_sector: SparkDataFrame = spark.table("gold.dim_sector")

    df = (
        fact.join(dim_time, on="time_key", how="left")
        .join(dim_sub_sector, on="sub_sector_key", how="left")
        .join(dim_sector, on="sector_key", how="left")
        .select(
            dim_time["year"],
            dim_time["quarter"],
            dim_sector["sector_name"],
            dim_sub_sector["sub_sector_name"],
            fact["unit"],
            fact["market_value"],
            fact["constant_value"],
            fact["market_value_pre_quarter"],
            fact["market_value_pre_year"],
            fact["constant_value_pre_quarter"],
            fact["constant_value_pre_year"],
            fact["market_qoq_growth_rate"],
            fact["market_yoy_growth_rate"],
            fact["real_qoq_growth_rate"],
            fact["real_yoy_growth_rate"],
            fact["implicit_price_deflator"],
            fact["sector_share_pct"],
            fact["gdp_share_pct"],
        )
    )

    df = df.withColumn(
        "quarter_label",
        F.concat(F.lit("Q"), F.col("quarter").cast("string"), F.lit(" "), F.col("year").cast("string")),
    )

    pdf = df.toPandas()
    return pdf


# ====================================================================
# FILTER OPTIONS & APPLY FILTERS
# ====================================================================

def get_filter_options(df: pd.DataFrame) -> dict[str, list[Any]]:
    """Lấy danh sách giá trị duy nhất cho từng bộ lọc.

    Args:
        df: DataFrame nguồn (chưa lọc).

    Returns:
        dict: Mapping tên filter -> danh sách giá trị (đã sort).
    """
    if df.empty:
        return {
            "year": [],
            "quarter": [],
            "sector": [],
            "sub_sector": [],
            "unit": [],
        }

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "quarter": sorted(df["quarter"].dropna().unique().tolist()),
        "sector": sorted(df["sector_name"].dropna().unique().tolist()),
        "sub_sector": sorted(df["sub_sector_name"].dropna().unique().tolist()),
        "unit": sorted(df["unit"].dropna().unique().tolist()),
    }


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    quarters: list[Any],
    sectors: list[Any],
    sub_sectors: list[Any],
    units: list[Any],
) -> pd.DataFrame:
    """Áp dụng các bộ lọc toàn cục lên DataFrame.

    Nếu một filter để trống (danh sách rỗng) thì coi như chọn tất cả
    giá trị của cột đó.

    Args:
        df: DataFrame nguồn.
        years: Danh sách năm được chọn.
        quarters: Danh sách quý được chọn.
        sectors: Danh sách sector được chọn.
        sub_sectors: Danh sách sub-sector được chọn.
        units: Danh sách đơn vị được chọn.

    Returns:
        pd.DataFrame: DataFrame đã lọc.
    """
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]
    if quarters:
        filtered = filtered[filtered["quarter"].isin(quarters)]
    if sectors:
        filtered = filtered[filtered["sector_name"].isin(sectors)]
    if sub_sectors:
        filtered = filtered[filtered["sub_sector_name"].isin(sub_sectors)]
    if units:
        filtered = filtered[filtered["unit"].isin(units)]

    return filtered


# ====================================================================
# RENDER FILTERS
# ====================================================================

def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render khu vực bộ lọc toàn cục và trả về DataFrame đã lọc.

    Args:
        df: DataFrame gốc (chưa lọc).

    Returns:
        pd.DataFrame: DataFrame sau khi áp dụng filter người dùng chọn.
    """
    options = get_filter_options(df)

    st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        years = st.multiselect("Year", options["year"], default=[])
    with col2:
        quarters = st.multiselect("Quarter", options["quarter"], default=[])
    with col3:
        sectors = st.multiselect("Sector", options["sector"], default=[])
    with col4:
        sub_sectors = st.multiselect("Sub-sector", options["sub_sector"], default=[])
    with col5:
        units = st.multiselect("Unit", options["unit"], default=[])

    st.markdown("</div>", unsafe_allow_html=True)

    return apply_filters(df, years, quarters, sectors, sub_sectors, units)


# ====================================================================
# KPI HELPERS
# ====================================================================

def _format_number(value: float) -> str:
    """Format số lớn theo dạng rút gọn (K, M, B, T)."""
    if pd.isna(value):
        return "N/A"

    abs_value = abs(value)
    sign = "-" if value < 0 else ""

    if abs_value >= 1_000_000_000_000:
        return f"{sign}{abs_value / 1_000_000_000_000:.2f}T"
    if abs_value >= 1_000_000_000:
        return f"{sign}{abs_value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{sign}{abs_value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{sign}{abs_value / 1_000:.2f}K"
    return f"{sign}{abs_value:.2f}"


def _format_percent(value: float) -> str:
    """Format số dạng phần trăm với 2 chữ số thập phân."""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}%"


def _kpi_card(label: str, value: str, css_class: str = "") -> str:
    """Tạo HTML cho một KPI card.

    Args:
        label: Tên KPI.
        value: Giá trị hiển thị (đã format).
        css_class: Class CSS bổ sung (vd: kpi-positive / kpi-negative).

    Returns:
        str: Chuỗi HTML của KPI card.
    """
    return f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value {css_class}">{value}</div>
        </div>
    """


def render_kpis(df: pd.DataFrame) -> None:
    """Render 6 KPI cards: Market GDP, Real GDP, QoQ, YoY, GDP Share, Top Growing Sub-sector.

    Args:
        df: DataFrame đã được lọc theo filter hiện tại.
    """
    st.markdown('<div class="gdp-section-title">Tổng quan KPI</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="empty-state">Không có dữ liệu phù hợp với bộ lọc hiện tại.</div>', unsafe_allow_html=True)
        return

    market_gdp = df["market_value"].sum()
    real_gdp = df["constant_value"].sum()
    avg_qoq = df["real_qoq_growth_rate"].mean()
    avg_yoy = df["real_yoy_growth_rate"].mean()
    avg_gdp_share = df["gdp_share_pct"].mean()

    top_growing = "N/A"
    if not df["real_yoy_growth_rate"].dropna().empty:
        top_row = df.loc[df["real_yoy_growth_rate"].idxmax()]
        top_growing = f"{top_row['sub_sector_name']} ({top_row['real_yoy_growth_rate']:.2f}%)"

    qoq_class = "kpi-positive" if avg_qoq >= 0 else "kpi-negative"
    yoy_class = "kpi-positive" if avg_yoy >= 0 else "kpi-negative"

    cols = st.columns(6)
    kpi_data = [
        ("Market GDP", _format_number(market_gdp), ""),
        ("Real GDP", _format_number(real_gdp), ""),
        ("QoQ Growth", _format_percent(avg_qoq), qoq_class),
        ("YoY Growth", _format_percent(avg_yoy), yoy_class),
        ("GDP Share", _format_percent(avg_gdp_share), ""),
        ("Top Growing Sub-sector", top_growing, "kpi-positive"),
    ]

    for col, (label, value, css_class) in zip(cols, kpi_data):
        with col:
            st.markdown(_kpi_card(label, value, css_class), unsafe_allow_html=True)


# ====================================================================
# CHART STYLING HELPER
# ====================================================================

def _apply_chart_theme(fig: go.Figure, height: int = 380) -> go.Figure:
    """Áp dụng theme nền trắng đồng bộ cho mọi biểu đồ Plotly.

    Args:
        fig: Đối tượng Figure của Plotly.
        height: Chiều cao biểu đồ (px).

    Returns:
        go.Figure: Figure đã áp dụng theme.
    """
    fig.update_layout(
        plot_bgcolor=CHART_PLOT_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(color=CHART_FONT_COLOR, size=12),
        height=height,
        margin=dict(l=40, r=30, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def _empty_chart_placeholder(message: str = "Không có dữ liệu để hiển thị") -> None:
    """Hiển thị placeholder khi DataFrame rỗng."""
    st.markdown(f'<div class="empty-state">{message}</div>', unsafe_allow_html=True)


# ====================================================================
# CHART FUNCTIONS
# ====================================================================

def chart_gdp_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart: Market GDP & Real GDP theo Quarter."""
    grouped = (
        df.groupby(["year", "quarter"], as_index=False)
        .agg(market_value=("market_value", "sum"), constant_value=("constant_value", "sum"))
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["quarter_label"],
            y=grouped["market_value"],
            mode="lines+markers",
            name="Market GDP",
            line=dict(color=COLOR_ACCENT, width=3),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=grouped["quarter_label"],
            y=grouped["constant_value"],
            mode="lines+markers",
            name="Real GDP",
            line=dict(color=COLOR_POSITIVE, width=3),
        )
    )
    fig.update_layout(title="GDP Trend")
    return _apply_chart_theme(fig)


def chart_growth_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart 4 đường: Market QoQ, Market YoY, Real QoQ, Real YoY."""
    grouped = (
        df.groupby(["year", "quarter"], as_index=False)
        .agg(
            market_qoq_growth_rate=("market_qoq_growth_rate", "mean"),
            market_yoy_growth_rate=("market_yoy_growth_rate", "mean"),
            real_qoq_growth_rate=("real_qoq_growth_rate", "mean"),
            real_yoy_growth_rate=("real_yoy_growth_rate", "mean"),
        )
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    series_config = [
        ("market_qoq_growth_rate", "Market QoQ", COLOR_ACCENT),
        ("market_yoy_growth_rate", "Market YoY", COLOR_POSITIVE),
        ("real_qoq_growth_rate", "Real QoQ", COLOR_NEGATIVE),
        ("real_yoy_growth_rate", "Real YoY", "#F5B041"),
    ]

    fig = go.Figure()
    for column, name, color in series_config:
        fig.add_trace(
            go.Scatter(
                x=grouped["quarter_label"],
                y=grouped[column],
                mode="lines+markers",
                name=name,
                line=dict(color=color, width=2.5),
            )
        )
    fig.update_layout(title="Growth Trend")
    return _apply_chart_theme(fig)


def chart_gdp_by_sector(df: pd.DataFrame) -> go.Figure:
    """Vẽ Stacked Bar: Market GDP theo Quarter, group theo Sector."""
    grouped = (
        df.groupby(["year", "quarter", "sector_name"], as_index=False)
        .agg(market_value=("market_value", "sum"))
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    fig = px.bar(
        grouped,
        x="quarter_label",
        y="market_value",
        color="sector_name",
        barmode="stack",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="GDP by Sector",
        labels={"quarter_label": "Quarter", "market_value": "Market GDP", "sector_name": "Sector"},
    )
    return _apply_chart_theme(fig)


def chart_sector_share(df: pd.DataFrame) -> go.Figure:
    """Vẽ Donut Chart: Sector Share."""
    grouped = df.groupby("sector_name", as_index=False).agg(gdp_share_pct=("gdp_share_pct", "mean"))

    fig = px.pie(
        grouped,
        names="sector_name",
        values="gdp_share_pct",
        hole=0.55,
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Sector Share",
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return _apply_chart_theme(fig)


def chart_top10_growth(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 real_yoy_growth_rate."""
    grouped = (
        df.groupby("sub_sector_name", as_index=False)
        .agg(real_yoy_growth_rate=("real_yoy_growth_rate", "mean"))
        .sort_values("real_yoy_growth_rate", ascending=False)
        .head(10)
        .sort_values("real_yoy_growth_rate")
    )

    fig = px.bar(
        grouped,
        x="real_yoy_growth_rate",
        y="sub_sector_name",
        orientation="h",
        color="real_yoy_growth_rate",
        color_continuous_scale=[COLOR_NEGATIVE, COLOR_ACCENT, COLOR_POSITIVE],
        title="Top 10 Growth (Real YoY)",
        labels={"real_yoy_growth_rate": "Real YoY Growth (%)", "sub_sector_name": "Sub-sector"},
    )
    fig.update_coloraxes(showscale=False)
    return _apply_chart_theme(fig)


def chart_top_gdp_share(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 gdp_share_pct."""
    grouped = (
        df.groupby("sub_sector_name", as_index=False)
        .agg(gdp_share_pct=("gdp_share_pct", "mean"))
        .sort_values("gdp_share_pct", ascending=False)
        .head(10)
        .sort_values("gdp_share_pct")
    )

    fig = px.bar(
        grouped,
        x="gdp_share_pct",
        y="sub_sector_name",
        orientation="h",
        color_discrete_sequence=[COLOR_ACCENT],
        title="Top GDP Share",
        labels={"gdp_share_pct": "GDP Share (%)", "sub_sector_name": "Sub-sector"},
    )
    return _apply_chart_theme(fig)


def chart_treemap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Treemap: Sector -> Sub-sector -> GDP Share."""
    grouped = (
        df.groupby(["sector_name", "sub_sector_name"], as_index=False)
        .agg(gdp_share_pct=("gdp_share_pct", "sum"))
    )
    grouped = grouped[grouped["gdp_share_pct"] > 0]

    fig = px.treemap(
        grouped,
        path=["sector_name", "sub_sector_name"],
        values="gdp_share_pct",
        color="sector_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Sector Structure (Treemap)",
    )
    return _apply_chart_theme(fig, height=480)


def chart_scatter_qoq_yoy(df: pd.DataFrame) -> go.Figure:
    """Vẽ Scatter: QoQ vs YoY, bubble size = GDP Share, color = Sector."""
    grouped = (
        df.groupby(["sub_sector_name", "sector_name"], as_index=False)
        .agg(
            real_qoq_growth_rate=("real_qoq_growth_rate", "mean"),
            real_yoy_growth_rate=("real_yoy_growth_rate", "mean"),
            gdp_share_pct=("gdp_share_pct", "mean"),
        )
    )
    grouped["bubble_size"] = grouped["gdp_share_pct"].abs().fillna(0) + 1

    fig = px.scatter(
        grouped,
        x="real_qoq_growth_rate",
        y="real_yoy_growth_rate",
        size="bubble_size",
        color="sector_name",
        hover_name="sub_sector_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="QoQ vs YoY Growth",
        labels={
            "real_qoq_growth_rate": "QoQ Growth (%)",
            "real_yoy_growth_rate": "YoY Growth (%)",
            "sector_name": "Sector",
        },
    )
    return _apply_chart_theme(fig)


def chart_heatmap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Heatmap: Sub-sector (rows) x Quarter (columns) = real_yoy_growth_rate."""
    grouped = df.copy()
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    pivot = grouped.pivot_table(
        index="sub_sector_name",
        columns="quarter_label",
        values="real_yoy_growth_rate",
        aggfunc="mean",
    )

    fig = px.imshow(
        pivot,
        color_continuous_scale=[COLOR_NEGATIVE, "#FFFFFF", COLOR_POSITIVE],
        aspect="auto",
        title="Growth by Quarter (Heatmap)",
        labels=dict(x="Quarter", y="Sub-sector", color="Real YoY (%)"),
    )
    return _apply_chart_theme(fig, height=460)


# ====================================================================
# RENDER SECTIONS
# ====================================================================

def _chart_card(render_fn, df: pd.DataFrame) -> None:
    """Wrapper render một biểu đồ trong khung card, xử lý trường hợp rỗng.

    Args:
        render_fn: Hàm tạo Figure (nhận DataFrame, trả về go.Figure).
        df: DataFrame đầu vào cho biểu đồ.
    """
    st.markdown('<div class="gdp-chart-wrapper">', unsafe_allow_html=True)
    if df.empty:
        _empty_chart_placeholder()
    else:
        fig = render_fn(df)
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_trend_section(df: pd.DataFrame) -> None:
    """Render Row 1: GDP Trend (Line) & Growth Trend (Line)."""
    st.markdown('<div class="gdp-section-title">Xu hướng GDP & Tăng trưởng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_gdp_trend, df)
    with col2:
        _chart_card(chart_growth_trend, df)


def render_structure_section(df: pd.DataFrame) -> None:
    """Render Row 2 (GDP by Sector, Sector Share) và Row 4 (Treemap)."""
    st.markdown('<div class="gdp-section-title">Cơ cấu GDP theo Sector</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_gdp_by_sector, df)
    with col2:
        _chart_card(chart_sector_share, df)

    st.markdown('<div class="gdp-section-title">Cấu trúc Sector (Treemap)</div>', unsafe_allow_html=True)
    _chart_card(chart_treemap, df)


def render_ranking_section(df: pd.DataFrame) -> None:
    """Render Row 3: Top 10 Growth & Top GDP Share (Horizontal Bar)."""
    st.markdown('<div class="gdp-section-title">Bảng xếp hạng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_top10_growth, df)
    with col2:
        _chart_card(chart_top_gdp_share, df)


def render_growth_section(df: pd.DataFrame) -> None:
    """Render Row 5: Scatter (QoQ vs YoY) & Heatmap (Growth by Quarter)."""
    st.markdown('<div class="gdp-section-title">Phân tích tăng trưởng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_scatter_qoq_yoy, df)
    with col2:
        _chart_card(chart_heatmap, df)


def render_drilldown_section(df: pd.DataFrame) -> None:
    """Render Row 6: Drill-down Sub-sector Analysis theo Sector được chọn."""
    st.markdown('<div class="gdp-section-title">Drill-down: Sub-sector Analysis</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    sectors = sorted(df["sector_name"].dropna().unique().tolist())
    if not sectors:
        st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
    selected_sector = st.selectbox("Chọn Sector để xem chi tiết Sub-sector", sectors)
    st.markdown("</div>", unsafe_allow_html=True)

    sector_df = df[df["sector_name"] == selected_sector]

    drilldown_table = (
        sector_df.groupby("sub_sector_name", as_index=False)
        .agg(
            market_gdp=("market_value", "sum"),
            real_gdp=("constant_value", "sum"),
            real_yoy_growth_rate=("real_yoy_growth_rate", "mean"),
            real_qoq_growth_rate=("real_qoq_growth_rate", "mean"),
            gdp_share_pct=("gdp_share_pct", "mean"),
        )
        .sort_values("gdp_share_pct", ascending=False)
        .rename(
            columns={
                "sub_sector_name": "Sub-sector",
                "market_gdp": "Market GDP",
                "real_gdp": "Real GDP",
                "real_yoy_growth_rate": "YoY Growth (%)",
                "real_qoq_growth_rate": "QoQ Growth (%)",
                "gdp_share_pct": "GDP Share (%)",
            }
        )
    )

    st.markdown('<div class="gdp-chart-wrapper">', unsafe_allow_html=True)
    if drilldown_table.empty:
        _empty_chart_placeholder()
    else:
        st.dataframe(
            drilldown_table.style.format(
                {
                    "Market GDP": "{:,.2f}",
                    "Real GDP": "{:,.2f}",
                    "YoY Growth (%)": "{:.2f}",
                    "QoQ Growth (%)": "{:.2f}",
                    "GDP Share (%)": "{:.2f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


# ====================================================================
# MAIN RENDER FUNCTION
# ====================================================================

def render_dashboard() -> None:
    """Render toàn bộ GDP Growth Performance Dashboard.

    Đây là entry point duy nhất được gọi từ tabs/gdp.py. Hàm này
    điều phối toàn bộ flow: inject CSS -> load data -> filter ->
    KPI -> các section biểu đồ -> drill-down.
    """
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không thể tải dữ liệu từ gold.fact_gdp_growth.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    filtered_df = render_filters(raw_df)

    render_kpis(filtered_df)
    render_trend_section(filtered_df)
    render_structure_section(filtered_df)
    render_ranking_section(filtered_df)
    render_growth_section(filtered_df)
    render_drilldown_section(filtered_df)