"""
Crop Yield Performance Dashboard.

Module này chứa toàn bộ logic của dashboard Crop Yield:
    - Truy vấn dữ liệu từ Spark (Gold layer).
    - Join các bảng dimension (dim_time, dim_crop).
    - Áp dụng bộ lọc (filter) toàn cục, trong đó Crop Name phụ thuộc
      Crop Category.
    - Tính toán KPI.
    - Vẽ toàn bộ biểu đồ bằng Plotly.
    - Inject CSS đồng bộ hoàn toàn với GDP Growth Dashboard.

app.py và tabs/crop.py không được chứa logic xử lý dữ liệu;
toàn bộ nằm trong file này, được expose qua hàm `render_dashboard()`.
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from shared.spark import get_spark_session

# ====================================================================
# THEME / COLOR CONSTANTS (đồng bộ với GDP Growth Dashboard)
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

CHART_PLOT_BG = "#07063C"
CHART_PAPER_BG = "#07063C"
CHART_FONT_COLOR = "#FFFFFF"


# ====================================================================
# CSS INJECTION
# ====================================================================

def inject_custom_css() -> None:
    """Inject CSS tuỳ chỉnh cho toàn bộ dashboard Crop Yield.

    Sử dụng cùng class name và token màu với GDP Growth Dashboard để
    đảm bảo giao diện đồng bộ hoàn toàn (font, khoảng cách section,
    card bo góc, shadow nhẹ, chart nền trắng).
    """
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .crop-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .crop-header h1 {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .crop-header p {{
            color: #E7F1FE;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .crop-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 14px;
        }}

        .crop-section-title {{
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

        .crop-chart-wrapper {{
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

        .streamlit-expanderHeader {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 10px;
            color: {COLOR_TEXT};
        }}

        div[data-testid="stMetric"] {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 12px 14px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
        }}

        div[data-testid="stMetricLabel"] {{
            color: {COLOR_TEXT_MUTED};
        }}

        div[data-testid="stMetricValue"] {{
            color: {COLOR_TEXT};
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
        <div class="crop-header">
            <h1>Crop Yield Performance Dashboard</h1>
            <p>Theo dõi sản lượng, năng suất và diện tích canh tác theo Crop Category / Crop Name / Year</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ====================================================================
# DATA LOADING (SPARK)
# ====================================================================

@st.cache_data(show_spinner="Đang tải dữ liệu Crop Yield...")
def load_data() -> pd.DataFrame:
    """Truy vấn dữ liệu Crop Yield từ Gold layer bằng Spark.

    Thực hiện join giữa fact_crop_yield với dim_time và dim_crop.
    Dữ liệu chỉ được convert sang Pandas ở bước cuối cùng (sau khi đã
    join xong bằng Spark), phục vụ cho việc filter/vẽ biểu đồ phía
    Streamlit.

    Returns:
        pd.DataFrame: Dữ liệu Crop Yield đã join đầy đủ dimension.
    """
    spark = get_spark_session()

    fact: SparkDataFrame = spark.table("gold.fact_crop_yield")
    dim_time: SparkDataFrame = spark.table("gold.dim_time")
    dim_crop: SparkDataFrame = spark.table("gold.dim_crop")

    df = (
        fact.join(dim_time, on="time_key", how="left")
        .join(dim_crop, on="crop_key", how="left")
        .select(
            dim_time["year"],
            dim_time["quarter"],
            dim_crop["crop_name"],
            dim_crop["crop_category"],
            fact["yield_unit"],
            fact["productivity_unit"],
            fact["area_unit"],
            fact["area"],
            fact["yield_value"],
            fact["productivity"],
            fact["area_pre_year"],
            fact["yield_pre_year"],
            fact["productivity_pre_year"],
            fact["productivity_yoy_growth_rate"],
            fact["productivity_share_pct"],
        )
    )

    pdf = df.toPandas()
    return pdf


# ====================================================================
# FILTER OPTIONS & APPLY FILTERS
# ====================================================================

def get_filter_options(df: pd.DataFrame, selected_categories: list[Any] | None = None) -> dict[str, list[Any]]:
    """Lấy danh sách giá trị duy nhất cho từng bộ lọc.

    Crop Name phụ thuộc Crop Category: nếu có category được chọn,
    danh sách crop_name chỉ gồm các crop thuộc category đó.

    Args:
        df: DataFrame nguồn (chưa lọc).
        selected_categories: Danh sách Crop Category đã chọn (dùng để
            lọc phụ thuộc cho Crop Name). None hoặc rỗng nghĩa là
            chưa chọn category nào -> hiển thị tất cả crop_name.

    Returns:
        dict: Mapping tên filter -> danh sách giá trị (đã sort).
    """
    if df.empty:
        return {
            "year": [],
            "crop_category": [],
            "crop_name": [],
            "yield_unit": [],
            "productivity_unit": [],
            "area_unit": [],
        }

    crop_name_pool = df
    if selected_categories:
        crop_name_pool = df[df["crop_category"].isin(selected_categories)]

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "crop_category": sorted(df["crop_category"].dropna().unique().tolist()),
        "crop_name": sorted(crop_name_pool["crop_name"].dropna().unique().tolist()),
        "yield_unit": sorted(df["yield_unit"].dropna().unique().tolist()),
        "productivity_unit": sorted(df["productivity_unit"].dropna().unique().tolist()),
        "area_unit": sorted(df["area_unit"].dropna().unique().tolist()),
    }


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    crop_categories: list[Any],
    crop_names: list[Any],
    yield_units: list[Any],
    productivity_unit: list[Any],
    area_units: list[Any],
) -> pd.DataFrame:
    """Áp dụng các bộ lọc toàn cục lên DataFrame.

    Nếu một filter để trống (danh sách rỗng) thì coi như chọn tất cả
    giá trị của cột đó.

    Args:
        df: DataFrame nguồn.
        years: Danh sách năm được chọn.
        crop_categories: Danh sách Crop Category được chọn.
        crop_names: Danh sách Crop Name được chọn.
        yield_units: Danh sách đơn vị sản lượng được chọn.
        productivity_unit: Danh sách đơn vị năng suất được chọn.
        area_units: Danh sách đơn vị diện tích được chọn.

    Returns:
        pd.DataFrame: DataFrame đã lọc.
    """
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]
    if crop_categories:
        filtered = filtered[filtered["crop_category"].isin(crop_categories)]
    if crop_names:
        filtered = filtered[filtered["crop_name"].isin(crop_names)]
    if yield_units:
        filtered = filtered[filtered["yield_unit"].isin(yield_units)]
    if productivity_unit:
        filtered = filtered[filtered["productivity_unit"].isin(productivity_unit)]
    if area_units:
        filtered = filtered[filtered["area_unit"].isin(area_units)]

    return filtered


# ====================================================================
# RENDER FILTERS
# ====================================================================

def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render khu vực bộ lọc toàn cục và trả về DataFrame đã lọc.

    Crop Name phụ thuộc Crop Category: danh sách option của Crop Name
    được cập nhật động dựa trên Crop Category đã chọn (sử dụng
    st.session_state để đọc giá trị Crop Category ngay trong cùng
    lượt render).

    Args:
        df: DataFrame gốc (chưa lọc).

    Returns:
        pd.DataFrame: DataFrame sau khi áp dụng filter người dùng chọn.
    """
    base_options = get_filter_options(df)

    st.markdown('<div class="crop-card">', unsafe_allow_html=True)
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        years = st.multiselect("Year", base_options["year"], default=[], key="crop_filter_year")
    with col2:
        crop_categories = st.multiselect(
            "Loại cây trồng", base_options["crop_category"], default=[], key="crop_filter_category"
        )

    # Crop Name phụ thuộc Crop Category đã chọn ở trên.
    dependent_options = get_filter_options(df, selected_categories=crop_categories)

    with col3:
        crop_names = st.multiselect(
            "Tên cây trồng", dependent_options["crop_name"], default=[], key="crop_filter_name"
        )
    with col4:
        yield_units = st.multiselect(
            "Đơn vị sản lượng", base_options["yield_unit"], default=[], key="crop_filter_production_unit"
        )
    with col5:
        productivity_unit = st.multiselect(
            "Đơn vị năng suất", base_options["productivity_unit"], default=[], key="crop_filter_yield_unit"
        )
    with col6:
        area_units = st.multiselect(
            "Đơn vị diện tích", base_options["area_unit"], default=[], key="crop_filter_area_unit"
        )

    st.markdown("</div>", unsafe_allow_html=True)

    return apply_filters(
        df, years, crop_categories, crop_names, yield_units, productivity_unit, area_units
    )


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
    """Render 6 KPI cards: Total Production, Total Area, Average Productivity,
    Average YoY Growth, Largest Crop Share, Top Producing Crop.

    Args:
        df: DataFrame đã được lọc theo filter hiện tại.
    """
    st.markdown('<div class="crop-section-title">Tổng quan KPI</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="empty-state">Không có dữ liệu phù hợp với bộ lọc hiện tại.</div>', unsafe_allow_html=True)
        return

    total_production = df["yield_value"].sum()
    total_area = df["area"].sum()
    avg_productivity = df["productivity"].mean()
    avg_yoy_growth = df["productivity_yoy_growth_rate"].mean()
    largest_share = df["productivity_share_pct"].mean()

    grouped = (
    df.groupby(["crop_category", "crop_name"], as_index=False)
        .agg(
            productivity=("productivity", "sum"),
            area=("area", "sum"),
            yield_value=("yield_value", "sum"),
            productivity_share_pct=("productivity_share_pct", "mean"),
        )
    )
    top_row = grouped.loc[grouped["productivity"].idxmax()]

    top_producing_crop = top_row["crop_name"]
    largest_share_top = top_row["productivity_share_pct"]
    cols = st.columns(6)
    kpi_data = [
        ("Tổng sản lượng (Nghìn tấn)", _format_number(total_production), ""),
        ("Tổng diện tích (Nghìn Ha)", _format_number(total_area), ""),
        ("Trung bình năng suất (Tạ/Ha)", _format_number(avg_productivity), ""),
        ("Trung bình phát triển năng suất qua từng năm", _format_percent(avg_yoy_growth),""),
        ("Sản phẩm năng suất cao nhất", top_producing_crop, "kpi-positive"),
        ("Tỷ trọng năng suất cây trồng lớn nhất", _format_percent(largest_share_top), ""),
        
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

def chart_production_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart: Production (Yield Value) theo Year, kèm Area trên trục phụ."""
    grouped = (
        df.groupby("year", as_index=False)
        .agg(yield_value=("yield_value", "sum"), area=("area", "sum"))
        .sort_values("year")
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["year"],
            y=grouped["yield_value"],
            mode="lines+markers",
            name="Production (Yield Value)",
            line=dict(color=COLOR_ACCENT, width=3),
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=grouped["year"],
            y=grouped["area"],
            mode="lines+markers",
            name="Area",
            line=dict(color=COLOR_POSITIVE, width=3, dash="dot"),
            yaxis="y2",
        )
    )
    fig.update_layout(
        title="Production Trend",
        xaxis=dict(title="Year"),
        yaxis=dict(title="Production"),
        yaxis2=dict(title="Area", overlaying="y", side="right", showgrid=False),
    )
    return _apply_chart_theme(fig)


def chart_productivity_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart: Average Productivity theo Year."""
    grouped = (
        df[df["productivity"] != 0]
        .groupby("year", as_index=False)
        .agg(productivity=("productivity", "mean"))
        .sort_values("year")
    )
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["year"],
            y=grouped["productivity"],
            mode="lines+markers",
            name="Average Productivity",
            line=dict(color=COLOR_ACCENT, width=3),
        )
    )
    fig.update_layout(title="Productivity Trend", xaxis=dict(title="Year"), yaxis=dict(title="Productivity"))
    return _apply_chart_theme(fig)


def chart_production_by_category(df: pd.DataFrame) -> go.Figure:
    """Vẽ Stacked Bar: Production theo Year, group theo Crop Category."""
    grouped = (
        df.groupby(["year", "crop_category"], as_index=False)
        .agg(yield_value=("yield_value", "sum"))
        .sort_values("year")
    )

    fig = px.bar(
        grouped,
        x="year",
        y="yield_value",
        color="crop_category",
        barmode="stack",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Production by Category",
        labels={"year": "Year", "yield_value": "Production", "crop_category": "Crop Category"},
    )
    return _apply_chart_theme(fig)


def chart_crop_share(df: pd.DataFrame) -> go.Figure:
    """Vẽ Donut Chart: Crop Share (Yield Share) theo Crop Category."""
    grouped = (
    df.groupby("crop_category", as_index=False)
    .agg(
        yield_value=("yield_value", "sum"),
        yield_unit=("yield_unit", "first"),
    )
    )

    fig = px.pie(
        grouped,
        names="crop_category",
        values="yield_value",
        hole=0.55,
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Crop Share",
        custom_data=["yield_unit"],
    )

    fig.update_traces(
        hovertemplate="""
    <b>%{label}</b><br>
    Sản lượng: %{value:,.0f} %{customdata[0]}<br>
    Tỷ trọng: %{percent}<extra></extra>
    """
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return _apply_chart_theme(fig)


def chart_top10_production(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 Crop theo Production (Yield Value)."""
    grouped = (
        df.groupby("crop_name", as_index=False)
        .agg(
            yield_value=("yield_value", "sum"),
            yield_unit=("yield_unit", "first"),
        )
        .sort_values("yield_value", ascending=False)
        .head(10)
        .sort_values("yield_value")
    )

    fig = px.bar(
        grouped,
        x="yield_value",
        y="crop_name",
        orientation="h",
        color_discrete_sequence=[COLOR_ACCENT],
        title="Top 10 Production",
        labels={
            "yield_value": "Production",
            "crop_name": "Crop",
        },
        custom_data=["yield_unit"],
    )

    fig.update_traces(
        texttemplate="%{x:,.0f}",
        textposition="outside",
        hovertemplate="""
    <b>%{y}</b><br>
    sản lượng: %{x:,.0f} %{customdata[0]}
    <extra></extra>
    """
    )
    return _apply_chart_theme(fig)


def chart_top10_productivity(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 Crop theo Productivity."""
    grouped = (
        df.groupby("crop_name", as_index=False)
        .agg(
            productivity=("productivity", "mean"),
            productivity_unit=("productivity_unit", "first"),
        )
    )

    grouped["productivity"] = grouped["productivity"].round(2)

    grouped = (
        grouped.sort_values("productivity", ascending=False)
        .head(10)
        .sort_values("productivity")
    )

    fig = px.bar(
        grouped,
        x="productivity",
        y="crop_name",
        orientation="h",
        color="productivity",
        color_continuous_scale=[COLOR_NEGATIVE, COLOR_ACCENT, COLOR_POSITIVE],
        custom_data=["productivity_unit"],
    )

    fig.update_coloraxes(showscale=False)

    fig.update_traces(
        texttemplate="%{x:.2f} %{customdata[0]}",
        textposition="outside",
        hovertemplate="""
    <b>%{y}</b><br>
    Productivity: %{x:.2f} %{customdata[0]}
    <extra></extra>
    """
    )
    return _apply_chart_theme(fig)


def chart_treemap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Treemap: Crop Category -> Crop Name (size = Yield)."""

    grouped = (
        df.groupby(
            ["crop_category", "crop_name"], as_index=False
        )
        .agg(
            productivity=("productivity", "mean"),
            area=("area", "sum"),
            yield_value=("yield_value", "sum"),
            productivity_unit=("productivity_unit", "first"),
            area_unit=("area_unit", "first"),
            yield_unit=("yield_unit", "first"),
        )
    )

    # Loại bỏ dữ liệu không hợp lệ
    grouped = grouped[grouped["yield_value"] > 0].copy()

    # Sắp xếp theo sản lượng giảm dần trong từng crop_category
    grouped = grouped.sort_values(
        ["crop_category", "yield_value"],
        ascending=[True, False],
        kind="stable",
    )

    # Tính tỷ trọng sản lượng trong từng crop_category
    grouped["yield_pct"] = (
        grouped["yield_value"]
        / grouped.groupby("crop_category")["yield_value"].transform("sum")
        * 100
    )

    fig = px.treemap(
        grouped,
        path=["crop_category", "crop_name"],
        values="yield_value",
        color="yield_pct",
        color_continuous_scale="Viridis",
        custom_data=[
            "yield_value",
            "yield_unit",
            "yield_pct",
            "productivity",
            "productivity_unit",
            "area",
            "area_unit",
        ],
    )

    fig.update_traces(
        texttemplate="<b>%{label}</b><br>%{customdata[2]:.1f}%",
        hovertemplate="""
    <b>%{label}</b><br>
    Crop Category: %{parent}<br><br>

    <b>Yield</b>: %{customdata[0]:,.2f} %{customdata[1]}<br>
    <b>Share in Category</b>: %{customdata[2]:.2f}%<br>
    <b>Productivity</b>: %{customdata[3]:,.2f} %{customdata[4]}<br>
    <b>Area</b>: %{customdata[5]:,.2f} %{customdata[6]}<br>

    <extra></extra>
    """
    )

    fig.update_layout(
        coloraxis_colorbar=dict(
            title="Yield Share (%)"
        ),
        margin=dict(t=40, l=10, r=10, b=10),
    )
    return _apply_chart_theme(fig, height=480)


def chart_area_vs_production(df: pd.DataFrame) -> go.Figure:
    """Vẽ Bubble Scatter: Area (X) vs Production (Y), size = Productivity, color = Crop Category."""
    grouped = (
        df.groupby(["crop_name", "crop_category"], as_index=False)
        .agg(
            area=("area", "sum"),
            yield_value=("yield_value", "sum"),
            productivity=("productivity", "mean"),
        )
    )
    grouped["bubble_size"] = grouped["productivity"].abs().fillna(0) + 1

    fig = px.scatter(
        grouped,
        x="area",
        y="yield_value",
        size="bubble_size",
        color="crop_category",
        hover_name="crop_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Area vs Production",
        labels={"area": "Area", "yield_value": "Production", "crop_category": "Crop Category"},
    )
    return _apply_chart_theme(fig)


def chart_growth_heatmap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Heatmap: Crop Name (rows) x Year (columns) = Yield YoY Growth Rate."""
    pivot = df.pivot_table(
        index="crop_name",
        columns="year",
        values="productivity_yoy_growth_rate",
        aggfunc="mean",
    )

    fig = px.imshow(
        pivot,
        color_continuous_scale=[COLOR_NEGATIVE, "#FFFFFF", COLOR_POSITIVE],
        aspect="auto",
        title="Growth Heatmap (Yield YoY Growth Rate)",
        labels=dict(x="Year", y="Crop", color="YoY Growth (%)"),
    )
    return _apply_chart_theme(fig, height=460)


def chart_current_vs_previous_year(df: pd.DataFrame) -> go.Figure:
    """Vẽ Grouped Bar: Current vs Previous Year cho Area, Production, Productivity.

    Mỗi metric được chuẩn hoá theo % so với giá trị lớn nhất của chính
    metric đó để có thể so sánh trực quan trên cùng một trục, do các
    metric (Area, Production, Productivity) có đơn vị và độ lớn khác
    nhau.
    """
    current = {
        "Area": df["area"].sum(),
        "Production": df["yield_value"].sum(),
        "Productivity": df["productivity"].mean(),
    }
    previous = {
        "Area": df["area_pre_year"].sum(),
        "Production": df["yield_pre_year"].sum(),
        "Productivity": df["productivity_pre_year"].mean(),
    }

    metrics = list(current.keys())

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=metrics,
            y=[current[m] for m in metrics],
            name="Current Year",
            marker_color=COLOR_ACCENT,
        )
    )
    fig.add_trace(
        go.Bar(
            x=metrics,
            y=[previous[m] for m in metrics],
            name="Previous Year",
            marker_color=COLOR_TEXT_MUTED,
        )
    )
    fig.update_layout(title="Current vs Previous Year", barmode="group")
    return _apply_chart_theme(fig)


# ====================================================================
# RENDER SECTIONS
# ====================================================================

def _chart_card(render_fn: Callable[[pd.DataFrame], go.Figure], df: pd.DataFrame) -> None:
    """Wrapper render một biểu đồ trong khung card, xử lý trường hợp rỗng.

    Args:
        render_fn: Hàm tạo Figure (nhận DataFrame, trả về go.Figure).
        df: DataFrame đầu vào cho biểu đồ.
    """
    st.markdown('<div class="crop-chart-wrapper">', unsafe_allow_html=True)
    if df.empty:
        _empty_chart_placeholder()
    else:
        fig = render_fn(df)
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_trend_section(df: pd.DataFrame) -> None:
    """Render Row 1: Production Trend (Line) & Productivity Trend (Line)."""
    st.markdown('<div class="crop-section-title">Xu hướng Sản lượng & Năng suất</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_production_trend, df)
    with col2:
        _chart_card(chart_productivity_trend, df)


def render_structure_section(df: pd.DataFrame) -> None:
    """Render Row 2 (Production by Category, Crop Share) và Row 4 (Treemap)."""
    st.markdown('<div class="crop-section-title">Cơ cấu Sản lượng theo Crop Category</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_production_by_category, df)
    with col2:
        _chart_card(chart_crop_share, df)

    st.markdown('<div class="crop-section-title">Cấu trúc Crop (Treemap)</div>', unsafe_allow_html=True)
    _chart_card(chart_treemap, df)


def render_ranking_section(df: pd.DataFrame) -> None:
    """Render Row 3: Top 10 Production & Top 10 Productivity (Horizontal Bar)."""
    st.markdown('<div class="crop-section-title">Bảng xếp hạng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_top10_production, df)
    with col2:
        _chart_card(chart_top10_productivity, df)


def render_analysis_section(df: pd.DataFrame) -> None:
    """Render Row 5: Area vs Production (Bubble Scatter) & Growth Heatmap."""
    st.markdown('<div class="crop-section-title">Phân tích Diện tích & Tăng trưởng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_area_vs_production, df)
    with col2:
        _chart_card(chart_growth_heatmap, df)


def render_comparison_section(df: pd.DataFrame) -> None:
    """Render Row 6: Current vs Previous Year (Grouped Bar)."""
    st.markdown('<div class="crop-section-title">So sánh Năm hiện tại & Năm trước</div>', unsafe_allow_html=True)
    _chart_card(chart_current_vs_previous_year, df)


def render_drilldown_section(df: pd.DataFrame) -> None:
    """Render Row 7: Crop Drill-down theo Crop Category được chọn.

    Hiển thị bảng chi tiết theo Crop Name gồm Production, Area,
    Productivity, Growth, Yield Share.
    """
    st.markdown('<div class="crop-section-title">Crop Drill-down</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="crop-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    categories = sorted(df["crop_category"].dropna().unique().tolist())
    if not categories:
        st.markdown('<div class="crop-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.markdown('<div class="crop-card">', unsafe_allow_html=True)
    selected_category = st.selectbox(
        "Chọn Crop Category để xem chi tiết Crop Name", categories, key="crop_drilldown_category"
    )
    st.markdown("</div>", unsafe_allow_html=True)

    category_df = df[df["crop_category"] == selected_category]

    drilldown_table = (
        category_df.groupby("crop_name", as_index=False)
        .agg(
            production=("yield_value", "sum"),
            area=("area", "sum"),
            productivity=("productivity", "mean"),
            productivity_yoy_growth_rate=("productivity_yoy_growth_rate", "mean"),
            productivity_share_pct=("productivity_share_pct", "mean"),
        )
        .sort_values("productivity_share_pct", ascending=False)
        .rename(
            columns={
                "crop_name": "Crop Name",
                "production": "Production",
                "area": "Area",
                "productivity": "Productivity",
                "productivity_yoy_growth_rate": "Growth (%)",
                "productivity_share_pct": "Yield Share (%)",
            }
        )
    )

    with st.expander("Xem chi tiết bảng dữ liệu", expanded=True):
        st.markdown('<div class="crop-chart-wrapper">', unsafe_allow_html=True)
        if drilldown_table.empty:
            _empty_chart_placeholder()
        else:
            st.dataframe(
                drilldown_table.style.format(
                    {
                        "Production": "{:,.2f}",
                        "Area": "{:,.2f}",
                        "Productivity": "{:,.2f}",
                        "Growth (%)": "{:.2f}",
                        "Yield Share (%)": "{:.2f}",
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
    """Render toàn bộ Crop Yield Performance Dashboard.

    Đây là entry point duy nhất được gọi từ tabs/crop.py. Hàm này
    điều phối toàn bộ flow: inject CSS -> load data -> filter ->
    KPI -> các section biểu đồ -> comparison -> drill-down.
    """
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        st.markdown('<div class="crop-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không thể tải dữ liệu từ gold.fact_crop_yield.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    filtered_df = render_filters(raw_df)

    render_kpis(filtered_df)
    render_trend_section(filtered_df)
    render_structure_section(filtered_df)
    render_ranking_section(filtered_df)
    # render_analysis_section(filtered_df)
    # render_comparison_section(filtered_df)
    render_drilldown_section(filtered_df)