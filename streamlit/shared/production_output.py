"""
National Production Output Dashboard.

Module này chứa toàn bộ logic của dashboard Production Output:
    - Truy vấn dữ liệu từ Spark (Gold layer).
    - Join các bảng dimension (dim_time, dim_product).
    - Áp dụng bộ lọc (filter) toàn cục, trong đó Product Type phụ
      thuộc Product Category, và Product Name phụ thuộc Product Type.
    - Tính toán KPI.
    - Vẽ toàn bộ biểu đồ bằng Plotly.
    - Inject CSS đồng bộ hoàn toàn với GDP Growth Dashboard và Crop
      Yield Dashboard.

app.py và tabs/production.py không được chứa logic xử lý dữ liệu;
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
# THEME / COLOR CONSTANTS (đồng bộ với GDP Growth & Crop Yield Dashboard)
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

CHART_PLOT_BG = "#0E0745"
CHART_PAPER_BG = "#0E0745"
CHART_FONT_COLOR = "#1A1A1A"


# ====================================================================
# CSS INJECTION
# ====================================================================

def inject_custom_css() -> None:
    """Inject CSS tuỳ chỉnh cho toàn bộ dashboard Production Output.

    Sử dụng cùng token màu, padding, border-radius, shadow với GDP
    Growth Dashboard và Crop Yield Dashboard để đảm bảo trải nghiệm
    người dùng đồng bộ hoàn toàn trên cả 3 dashboard.
    """
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .production-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .production-header h1 {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .production-header p {{
            color: #E7F1FE;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .production-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 14px;
        }}

        .production-section-title {{
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

        .production-chart-wrapper {{
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
        <div class="production-header">
            <h1>National Production Output Dashboard</h1>
            <p>Theo dõi sản lượng quốc gia theo Product Category / Type / Name và Quarter</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ====================================================================
# DATA LOADING (SPARK)
# ====================================================================

@st.cache_data(show_spinner="Đang tải dữ liệu Production Output...")
def load_data() -> pd.DataFrame:
    """Truy vấn dữ liệu Production Output từ Gold layer bằng Spark.

    Thực hiện join giữa fact_production_output với dim_time và
    dim_product. Dữ liệu chỉ được convert sang Pandas ở bước cuối
    cùng (sau khi đã join xong bằng Spark), phục vụ cho việc
    filter/vẽ biểu đồ phía Streamlit.

    Returns:
        pd.DataFrame: Dữ liệu Production Output đã join đầy đủ dimension.
    """
    spark = get_spark_session()

    fact: SparkDataFrame = spark.table("gold.fact_production_output")
    dim_time: SparkDataFrame = spark.table("gold.dim_time")
    dim_product: SparkDataFrame = spark.table("gold.dim_product")

    df = (
        fact.join(dim_time, on="time_key", how="left")
        .join(dim_product, on="product_key", how="left")
        .select(
            dim_time["year"],
            dim_time["quarter"],
            dim_product["product_name"],
            dim_product["product_type"],
            dim_product["product_category"],
            fact["value"],
            fact["unit"],
            fact["prev_quarter_value"],
            fact["pre_year_value"],
            fact["yoy_growth_rate"],
            fact["qoq_growth_rate"],
            fact["product_share_pct"],
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

def get_filter_options(
    df: pd.DataFrame,
    selected_categories: list[Any] | None = None,
    selected_types: list[Any] | None = None,
) -> dict[str, list[Any]]:
    """Lấy danh sách giá trị duy nhất cho từng bộ lọc.

    Product Type phụ thuộc Product Category, và Product Name phụ
    thuộc Product Type (cascading filter 2 cấp).

    Args:
        df: DataFrame nguồn (chưa lọc).
        selected_categories: Danh sách Product Category đã chọn, dùng
            để thu hẹp danh sách Product Type. None hoặc rỗng nghĩa
            là chưa chọn category nào -> hiển thị tất cả product_type.
        selected_types: Danh sách Product Type đã chọn, dùng để thu
            hẹp danh sách Product Name. None hoặc rỗng nghĩa là chưa
            chọn type nào -> hiển thị tất cả product_name (trong
            phạm vi category nếu có).

    Returns:
        dict: Mapping tên filter -> danh sách giá trị (đã sort).
    """
    if df.empty:
        return {
            "year": [],
            "quarter": [],
            "product_category": [],
            "product_type": [],
            "product_name": [],
            "unit": [],
        }

    type_pool = df
    if selected_categories:
        type_pool = type_pool[type_pool["product_category"].isin(selected_categories)]

    name_pool = type_pool
    if selected_types:
        name_pool = name_pool[name_pool["product_type"].isin(selected_types)]

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "quarter": sorted(df["quarter"].dropna().unique().tolist()),
        "product_category": sorted(df["product_category"].dropna().unique().tolist()),
        "product_type": sorted(type_pool["product_type"].dropna().unique().tolist()),
        "product_name": sorted(name_pool["product_name"].dropna().unique().tolist()),
        "unit": sorted(df["unit"].dropna().unique().tolist()),
    }


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    quarters: list[Any],
    product_categories: list[Any],
    product_types: list[Any],
    product_names: list[Any],
    units: list[Any],
) -> pd.DataFrame:
    """Áp dụng các bộ lọc toàn cục lên DataFrame.

    Nếu một filter để trống (danh sách rỗng) thì coi như chọn tất cả
    giá trị của cột đó.

    Args:
        df: DataFrame nguồn.
        years: Danh sách năm được chọn.
        quarters: Danh sách quý được chọn.
        product_categories: Danh sách Product Category được chọn.
        product_types: Danh sách Product Type được chọn.
        product_names: Danh sách Product Name được chọn.
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
    if product_categories:
        filtered = filtered[filtered["product_category"].isin(product_categories)]
    if product_types:
        filtered = filtered[filtered["product_type"].isin(product_types)]
    if product_names:
        filtered = filtered[filtered["product_name"].isin(product_names)]
    if units:
        filtered = filtered[filtered["unit"].isin(units)]

    return filtered


# ====================================================================
# RENDER FILTERS
# ====================================================================

def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render khu vực bộ lọc toàn cục và trả về DataFrame đã lọc.

    Cascading filter 2 cấp: Product Type phụ thuộc Product Category
    đã chọn; Product Name phụ thuộc Product Type đã chọn (và gián
    tiếp Product Category). Danh sách option được tính lại ngay
    trong cùng lượt render dựa trên lựa chọn trước đó.

    Args:
        df: DataFrame gốc (chưa lọc).

    Returns:
        pd.DataFrame: DataFrame sau khi áp dụng filter người dùng chọn.
    """
    base_options = get_filter_options(df)

    st.markdown('<div class="production-card">', unsafe_allow_html=True)
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        years = st.multiselect("Year", base_options["year"], default=[], key="production_filter_year")
    with col2:
        quarters = st.multiselect("Quarter", base_options["quarter"], default=[], key="production_filter_quarter")
    with col3:
        product_categories = st.multiselect(
            "Product Category", base_options["product_category"], default=[], key="production_filter_category"
        )

    # Product Type phụ thuộc Product Category đã chọn.
    type_options = get_filter_options(df, selected_categories=product_categories)

    with col4:
        product_types = st.multiselect(
            "Product Type", type_options["product_type"], default=[], key="production_filter_type"
        )

    # Product Name phụ thuộc Product Category + Product Type đã chọn.
    name_options = get_filter_options(
        df, selected_categories=product_categories, selected_types=product_types
    )

    with col5:
        product_names = st.multiselect(
            "Product Name", name_options["product_name"], default=[], key="production_filter_name"
        )
    with col6:
        units = st.multiselect("Unit", base_options["unit"], default=[], key="production_filter_unit")

    st.markdown("</div>", unsafe_allow_html=True)

    return apply_filters(
        df, years, quarters, product_categories, product_types, product_names, units
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
    """Render 6 KPI cards: Total Production Output, Average YoY Growth,
    Average QoQ Growth, Largest Product Category, Largest Product Share,
    Top Product.

    Args:
        df: DataFrame đã được lọc theo filter hiện tại.
    """
    st.markdown('<div class="production-section-title">Tổng quan KPI</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="empty-state">Không có dữ liệu phù hợp với bộ lọc hiện tại.</div>', unsafe_allow_html=True)
        return

    total_output = df["value"].sum()
    avg_yoy = df["yoy_growth_rate"].mean()
    avg_qoq = df["qoq_growth_rate"].mean()
    largest_share = df["product_share_pct"].max()

    largest_category = "N/A"
    category_totals = df.groupby("product_category")["value"].sum()
    if not category_totals.empty:
        largest_category = str(category_totals.idxmax())
        
    top_product = "N/A"
    product_totals = df.groupby("product_name")["value"].sum()
    if not product_totals.empty:
        top_product = str(product_totals.idxmax())

    unit_label = "N/A"

    unit_mode = df["unit"].mode()
    if not unit_mode.empty:
        unit_label = unit_mode.iloc[0]
    yoy_class = "kpi-positive" if avg_yoy >= 0 else "kpi-negative"
    qoq_class = "kpi-positive" if avg_qoq >= 0 else "kpi-negative"

    cols = st.columns(3)
    kpi_data = [
        ("Total Production Output", f"{_format_number(total_output)} {unit_label}", ""),
        ("Average YoY Growth", _format_percent(avg_yoy), yoy_class),
        ("Average QoQ Growth", _format_percent(avg_qoq), qoq_class),
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
    """Vẽ Line Chart: Production Value theo Quarter."""
    grouped = (
        df.groupby(["year", "quarter"], as_index=False)
        .agg(value=("value", "sum"))
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["quarter_label"],
            y=grouped["value"],
            mode="lines+markers",
            name="Production Value",
            line=dict(color=COLOR_ACCENT, width=3),
        )
    )
    fig.update_layout(title="Production Trend")
    return _apply_chart_theme(fig)


def chart_growth_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart 2 đường: QoQ Growth & YoY Growth theo Quarter."""
    grouped = (
        df.groupby(["year", "quarter"], as_index=False)
        .agg(
            qoq_growth_rate=("qoq_growth_rate", "mean"),
            yoy_growth_rate=("yoy_growth_rate", "mean"),
        )
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["quarter_label"],
            y=grouped["qoq_growth_rate"],
            mode="lines+markers",
            name="QoQ Growth",
            line=dict(color=COLOR_ACCENT, width=2.5),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=grouped["quarter_label"],
            y=grouped["yoy_growth_rate"],
            mode="lines+markers",
            name="YoY Growth",
            line=dict(color=COLOR_POSITIVE, width=2.5),
        )
    )
    fig.update_layout(title="Growth Trend")
    return _apply_chart_theme(fig)


def chart_production_by_category(df: pd.DataFrame) -> go.Figure:
    """Vẽ Stacked Bar: Production theo Quarter, group theo Product Category."""
    grouped = (
        df.groupby(["year", "quarter", "product_category"], as_index=False)
        .agg(value=("value", "sum"))
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    fig = px.bar(
        grouped,
        x="quarter_label",
        y="value",
        color="product_category",
        barmode="stack",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Production by Category",
        labels={"quarter_label": "Quarter", "value": "Production", "product_category": "Product Category"},
    )
    return _apply_chart_theme(fig)


def chart_production_share(df: pd.DataFrame) -> go.Figure:
    """Vẽ Donut Chart: Production Share theo Product Category."""
    grouped = df.groupby(["product_name"], as_index=False).agg(product_share_pct=("value", "mean"))

    fig = px.pie(
        grouped,
        names="product_name",
        values="product_share_pct",
        hole=0.55,
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Production Share",
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return _apply_chart_theme(fig)


def chart_top10_production(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 Product theo Production Value."""
    grouped = (
        df.groupby("product_name", as_index=False)
        .agg(value=("value", "sum"))
        .sort_values("value", ascending=False)
        .head(10)
        .sort_values("value")
    )

    fig = px.bar(
        grouped,
        x="value",
        y="product_name",
        orientation="h",
        color_discrete_sequence=[COLOR_ACCENT],
        title="Top 10 Production",
        labels={"value": "Production", "product_name": "Product"},
    )
    return _apply_chart_theme(fig)


def chart_top10_growth(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 Product theo YoY Growth Rate."""
    grouped = (
        df.groupby("product_name", as_index=False)
        .agg(yoy_growth_rate=("yoy_growth_rate", "mean"))
        .sort_values("yoy_growth_rate", ascending=False)
        .head(10)
        .sort_values("yoy_growth_rate")
    )

    fig = px.bar(
        grouped,
        x="yoy_growth_rate",
        y="product_name",
        orientation="h",
        color="yoy_growth_rate",
        color_continuous_scale=[COLOR_NEGATIVE, COLOR_ACCENT, COLOR_POSITIVE],
        title="Top 10 Growth (YoY)",
        labels={"yoy_growth_rate": "YoY Growth (%)", "product_name": "Product"},
    )
    fig.update_coloraxes(showscale=False)
    return _apply_chart_theme(fig)


def chart_treemap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Treemap: Product Category -> Product Type -> Product Name.

    Kích thước ô theo Production Value, màu sắc theo YoY Growth Rate.
    """
    grouped = (
        df.groupby(
            [
                "product_category",
                "unit",
                "product_type",
                "product_name",
            ],
            as_index=False,
        )
        .agg(
            value=("value", "sum"),
        )
    )
    grouped["share"] = (
        grouped["value"]
        / grouped.groupby(["product_category", "unit"])["value"].transform("sum")
    )
    grouped = grouped[grouped["value"] > 0]

    fig = px.treemap(
        grouped,
        path=[
            "product_category",
            "unit",
            "product_type",
            "product_name",
        ],
        values="share",
        color="product_name",  # Mỗi đơn vị một màu (có thể bỏ nếu không muốn)
        title="Production Structure (Treemap)",
        custom_data=[
            "value",
            "unit",
        ],
    )

    fig.update_traces(
        texttemplate="<b>%{label}</b>",
        hovertemplate="""
    <b>%{label}</b><br>
    Category: %{parent}<br>
    Production: %{customdata[0]:,.2f} %{customdata[1]}
    <extra></extra>
    """
    )

    fig.update_layout(
        margin=dict(t=50, l=10, r=10, b=10),
    )
    return _apply_chart_theme(fig, height=480)


def chart_qoq_vs_yoy(df: pd.DataFrame) -> go.Figure:
    """Vẽ Bubble Scatter: QoQ Growth (X) vs YoY Growth (Y), size = Production, color = Category."""
    grouped = (
        df.groupby(["product_name", "product_category"], as_index=False)
        .agg(
            qoq_growth_rate=("qoq_growth_rate", "mean"),
            yoy_growth_rate=("yoy_growth_rate", "mean"),
            value=("value", "sum"),
        )
    )
    grouped["bubble_size"] = grouped["value"].abs().fillna(0) + 1

    fig = px.scatter(
        grouped,
        x="qoq_growth_rate",
        y="yoy_growth_rate",
        size="bubble_size",
        color="product_category",
        hover_name="product_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="QoQ vs YoY Growth",
        labels={
            "qoq_growth_rate": "QoQ Growth (%)",
            "yoy_growth_rate": "YoY Growth (%)",
            "product_category": "Product Category",
        },
    )
    return _apply_chart_theme(fig)


def chart_growth_heatmap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Heatmap: Product Name (rows) x Quarter (columns) = YoY Growth Rate."""
    grouped = df.copy()
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    pivot = grouped.pivot_table(
        index="product_name",
        columns="quarter_label",
        values="yoy_growth_rate",
        aggfunc="mean",
    )

    fig = px.imshow(
        pivot,
        color_continuous_scale=[COLOR_NEGATIVE, "#FFFFFF", COLOR_POSITIVE],
        aspect="auto",
        title="Growth Heatmap (YoY)",
        labels=dict(x="Quarter", y="Product", color="YoY Growth (%)"),
    )
    return _apply_chart_theme(fig, height=460)


def chart_current_vs_previous(df: pd.DataFrame) -> go.Figure:
    """Vẽ Grouped Bar: Current vs Previous Quarter vs Previous Year.

    Sử dụng value, prev_quarter_value, pre_year_value.
    """
    current = df["value"].sum()
    prev_quarter = df["prev_quarter_value"].sum()
    prev_year = df["pre_year_value"].sum()

    labels = ["Current", "Previous Quarter", "Previous Year"]
    values = [current, prev_quarter, prev_year]
    colors = [COLOR_ACCENT, COLOR_POSITIVE, COLOR_TEXT_MUTED]

    fig = go.Figure(
        data=[
            go.Bar(
                x=labels,
                y=values,
                marker_color=colors,
                text=[f"{v:,.2f}" for v in values],
                textposition="outside",
            )
        ]
    )
    fig.update_layout(title="Current vs Previous Comparison")
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
    st.markdown('<div class="production-chart-wrapper">', unsafe_allow_html=True)
    if df.empty:
        _empty_chart_placeholder()
    else:
        fig = render_fn(df)
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_trend_section(df: pd.DataFrame) -> None:
    """Render Row 1: Production Trend (Line) & Growth Trend (Line)."""
    st.markdown('<div class="production-section-title">Xu hướng Sản lượng & Tăng trưởng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_production_trend, df)
    with col2:
        _chart_card(chart_growth_trend, df)


def render_structure_section(df: pd.DataFrame) -> None:
    """Render Row 2 (Production by Category, Production Share) và Row 4 (Treemap)."""
    st.markdown('<div class="production-section-title">Cơ cấu Sản lượng theo Product Category</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_production_by_category, df)
    with col2:
        _chart_card(chart_production_share, df)

    st.markdown('<div class="production-section-title">Cấu trúc Sản phẩm (Treemap)</div>', unsafe_allow_html=True)
    _chart_card(chart_treemap, df)


def render_growth_section(df: pd.DataFrame) -> None:
    """Render Row 5: QoQ vs YoY (Bubble Scatter) & Growth Heatmap."""
    st.markdown('<div class="production-section-title">Phân tích Tăng trưởng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_qoq_vs_yoy, df)
    with col2:
        _chart_card(chart_growth_heatmap, df)


def render_ranking_section(df: pd.DataFrame) -> None:
    """Render Row 3: Top 10 Production & Top 10 Growth (Horizontal Bar)."""
    st.markdown('<div class="production-section-title">Bảng xếp hạng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_top10_production, df)
    with col2:
        _chart_card(chart_top10_growth, df)


def render_comparison_section(df: pd.DataFrame) -> None:
    """Render Row 6: Current vs Previous Quarter vs Previous Year (Grouped Bar)."""
    st.markdown('<div class="production-section-title">So sánh Current / Previous Quarter / Previous Year</div>', unsafe_allow_html=True)
    _chart_card(chart_current_vs_previous, df)


def render_drilldown_section(df: pd.DataFrame) -> None:
    """Render Row 7: Product Drill-down theo Product Category được chọn.

    Hiển thị bảng chi tiết Product Type -> Product Name gồm
    Production, QoQ, YoY, Production Share.
    """
    st.markdown('<div class="production-section-title">Product Drill-down</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="production-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    categories = sorted(df["product_category"].dropna().unique().tolist())
    if not categories:
        st.markdown('<div class="production-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.markdown('<div class="production-card">', unsafe_allow_html=True)
    selected_category = st.selectbox(
        "Chọn Product Category để xem chi tiết", categories, key="production_drilldown_category"
    )
    st.markdown("</div>", unsafe_allow_html=True)

    category_df = df[df["product_category"] == selected_category]

    drilldown_table = (
        category_df.groupby(["product_type", "product_name"], as_index=False)
        .agg(
            production=("value", "sum"),
            qoq_growth_rate=("qoq_growth_rate", "mean"),
            yoy_growth_rate=("yoy_growth_rate", "mean"),
            product_share_pct=("product_share_pct", "mean"),
        )
        .sort_values("product_share_pct", ascending=False)
        .rename(
            columns={
                "product_type": "Product Type",
                "product_name": "Product Name",
                "production": "Production",
                "qoq_growth_rate": "QoQ Growth (%)",
                "yoy_growth_rate": "YoY Growth (%)",
                "product_share_pct": "Production Share (%)",
            }
        )
    )

    with st.expander("Xem chi tiết bảng dữ liệu", expanded=True):
        st.markdown('<div class="production-chart-wrapper">', unsafe_allow_html=True)
        if drilldown_table.empty:
            _empty_chart_placeholder()
        else:
            st.dataframe(
                drilldown_table.style.format(
                    {
                        "Production": "{:,.2f}",
                        "QoQ Growth (%)": "{:.2f}",
                        "YoY Growth (%)": "{:.2f}",
                        "Production Share (%)": "{:.2f}",
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
    """Render toàn bộ National Production Output Dashboard.

    Đây là entry point duy nhất được gọi từ tabs/production.py. Hàm
    này điều phối toàn bộ flow: inject CSS -> load data -> filter ->
    KPI -> các section biểu đồ -> comparison -> drill-down.
    """
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        st.markdown('<div class="production-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không thể tải dữ liệu từ gold.fact_production_output.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    filtered_df = render_filters(raw_df)

    render_kpis(filtered_df)
    render_trend_section(filtered_df)
    render_structure_section(filtered_df)
    # render_ranking_section(filtered_df)
    # render_growth_section(filtered_df)
    # render_comparison_section(filtered_df)
    # render_drilldown_section(filtered_df)