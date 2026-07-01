"""
International Trade Performance Dashboard.

Module này chứa toàn bộ logic của dashboard International Trade:
    - Truy vấn dữ liệu từ Spark (Gold layer).
    - Join các bảng dimension (dim_time, dim_product).
    - Áp dụng bộ lọc (filter) toàn cục, trong đó Product Name phụ
      thuộc Product Category. Trade Type (Export/Import) được suy ra
      từ dim_product.product_type.
    - Tính toán KPI, bao gồm Average Unit Value = SUM(trade_value) / SUM(quantity).
    - Vẽ toàn bộ biểu đồ bằng Plotly.
    - Inject CSS đồng bộ hoàn toàn với GDP Growth, Crop Yield và
      Production Output Dashboard.

app.py và tabs/trade.py không được chứa logic xử lý dữ liệu;
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
# THEME / COLOR CONSTANTS (đồng bộ với GDP / Crop / Production Dashboard)
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

CHART_PLOT_BG = "#07082E"
CHART_PAPER_BG = "#07082E"
CHART_FONT_COLOR = "#1A1A1A"


# ====================================================================
# CSS INJECTION
# ====================================================================

def inject_custom_css() -> None:
    """Inject CSS tuỳ chỉnh cho toàn bộ dashboard International Trade.

    Sử dụng cùng token màu, padding, border-radius, shadow với GDP
    Growth, Crop Yield và Production Output Dashboard để đảm bảo trải
    nghiệm người dùng đồng bộ hoàn toàn trên cả 4 dashboard.
    """
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .trade-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .trade-header h1 {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .trade-header p {{
            color: #E7F1FE;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .trade-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 14px;
        }}

        .trade-section-title {{
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
            font-size: 22px;
            font-weight: 700;
        }}

        .kpi-positive {{
            color: {COLOR_POSITIVE};
        }}

        .kpi-negative {{
            color: {COLOR_NEGATIVE};
        }}

        .trade-chart-wrapper {{
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
        <div class="trade-header">
            <h1>International Trade Performance Dashboard</h1>
            <p>Theo dõi kim ngạch xuất nhập khẩu theo Trade Type / Product Category / Product Name và Month</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ====================================================================
# DATA LOADING (SPARK)
# ====================================================================

@st.cache_data(show_spinner="Đang tải dữ liệu International Trade...")
def load_data() -> pd.DataFrame:
    """Truy vấn dữ liệu International Trade từ Gold layer bằng Spark.

    Thực hiện join giữa fact_international_trade với dim_time và
    dim_product. Cột `product_type` của dim_product được dùng làm
    "Trade Type" (Export / Import) theo đúng business context. Dữ
    liệu chỉ được convert sang Pandas ở bước cuối cùng (sau khi đã
    join xong bằng Spark), phục vụ cho việc filter/vẽ biểu đồ phía
    Streamlit.

    Returns:
        pd.DataFrame: Dữ liệu International Trade đã join đầy đủ dimension.
    """
    spark = get_spark_session()

    fact: SparkDataFrame = spark.table("gold.fact_international_trade")
    dim_time: SparkDataFrame = spark.table("gold.dim_time")
    dim_product: SparkDataFrame = spark.table("gold.dim_product")

    df = (
        fact.join(dim_time, on="time_key", how="left")
        .join(dim_product, on="product_key", how="left")
        .select(
            dim_time["year"],
            dim_time["month"],
            dim_product["product_name"],
            dim_product["product_type"].alias("trade_type"),
            dim_product["product_category"],
            fact["trade_value"],
            fact["value_unit"],
            fact["quantity"],
            fact["quantity_unit"],
            fact["trade_value_pre_month"],
            fact["trade_value_pre_year"],
            fact["mom_growth_rate"],
            fact["yoy_growth_rate"],
            fact["product_share_pct"],
        )
    )

    df = df.withColumn(
        "month_label",
        F.concat(F.col("year").cast("string"), F.lit("-"), F.lpad(F.col("month").cast("string"), 2, "0")),
    )

    pdf = df.toPandas()
    return pdf


# ====================================================================
# FILTER OPTIONS & APPLY FILTERS
# ====================================================================

def get_filter_options(
    df: pd.DataFrame, selected_categories: list[Any] | None = None
) -> dict[str, list[Any]]:
    """Lấy danh sách giá trị duy nhất cho từng bộ lọc.

    Product Name phụ thuộc Product Category (không phụ thuộc Trade
    Type, theo đúng yêu cầu).

    Args:
        df: DataFrame nguồn (chưa lọc).
        selected_categories: Danh sách Product Category đã chọn, dùng
            để thu hẹp danh sách Product Name. None hoặc rỗng nghĩa
            là chưa chọn category nào -> hiển thị tất cả product_name.

    Returns:
        dict: Mapping tên filter -> danh sách giá trị (đã sort).
    """
    if df.empty:
        return {
            "year": [],
            "month": [],
            "trade_type": [],
            "product_category": [],
            "product_name": [],
            "value_unit": [],
            "quantity_unit": [],
        }

    name_pool = df
    if selected_categories:
        name_pool = name_pool[name_pool["product_category"].isin(selected_categories)]

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "month": sorted(df["month"].dropna().unique().tolist()),
        "trade_type": sorted(df["trade_type"].dropna().unique().tolist()),
        "product_category": sorted(df["product_category"].dropna().unique().tolist()),
        "product_name": sorted(name_pool["product_name"].dropna().unique().tolist()),
        "value_unit": sorted(df["value_unit"].dropna().unique().tolist()),
        "quantity_unit": sorted(df["quantity_unit"].dropna().unique().tolist()),
    }


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    months: list[Any],
    trade_types: list[Any],
    product_categories: list[Any],
    product_names: list[Any],
    value_units: list[Any],
    quantity_units: list[Any],
) -> pd.DataFrame:
    """Áp dụng các bộ lọc toàn cục lên DataFrame.

    Nếu một filter để trống (danh sách rỗng) thì coi như chọn tất cả
    giá trị của cột đó.

    Args:
        df: DataFrame nguồn.
        years: Danh sách năm được chọn.
        months: Danh sách tháng được chọn.
        trade_types: Danh sách Trade Type (Export/Import) được chọn.
        product_categories: Danh sách Product Category được chọn.
        product_names: Danh sách Product Name được chọn.
        value_units: Danh sách đơn vị giá trị được chọn.
        quantity_units: Danh sách đơn vị khối lượng được chọn.

    Returns:
        pd.DataFrame: DataFrame đã lọc.
    """
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]
    if months:
        filtered = filtered[filtered["month"].isin(months)]
    if trade_types:
        filtered = filtered[filtered["trade_type"].isin(trade_types)]
    if product_categories:
        filtered = filtered[filtered["product_category"].isin(product_categories)]
    if product_names:
        filtered = filtered[filtered["product_name"].isin(product_names)]
    if value_units:
        filtered = filtered[filtered["value_unit"].isin(value_units)]
    if quantity_units:
        filtered = filtered[filtered["quantity_unit"].isin(quantity_units)]

    return filtered


# ====================================================================
# AVERAGE UNIT VALUE CALCULATION
# ====================================================================

def calculate_average_unit_value(df: pd.DataFrame) -> float:
    """Tính Average Unit Value = SUM(trade_value) / SUM(quantity).

    Trả về NaN nếu DataFrame rỗng hoặc tổng quantity bằng 0, để tránh
    chia cho 0.

    Args:
        df: DataFrame đã lọc.

    Returns:
        float: Giá trị trung bình trên mỗi đơn vị hàng hoá, hoặc NaN
            nếu không tính được.
    """
    if df.empty:
        return float("nan")

    total_quantity = df["quantity"].sum()
    if total_quantity == 0 or pd.isna(total_quantity):
        return float("nan")

    total_trade_value = df["trade_value"].sum()
    return total_trade_value / total_quantity


# ====================================================================
# RENDER FILTERS
# ====================================================================

def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render khu vực bộ lọc toàn cục và trả về DataFrame đã lọc.

    Product Name phụ thuộc Product Category đã chọn. Danh sách option
    được tính lại ngay trong cùng lượt render dựa trên lựa chọn
    Product Category trước đó.

    Args:
        df: DataFrame gốc (chưa lọc).

    Returns:
        pd.DataFrame: DataFrame sau khi áp dụng filter người dùng chọn.
    """
    base_options = get_filter_options(df)

    st.markdown('<div class="trade-card">', unsafe_allow_html=True)
    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)

    with col1:
        years = st.multiselect("Year", base_options["year"], default=[], key="trade_filter_year")
    with col2:
        months = st.multiselect("Month", base_options["month"], default=[], key="trade_filter_month")
    with col3:
        trade_types = st.multiselect(
            "Trade Type", base_options["trade_type"], default=[], key="trade_filter_trade_type"
        )
    with col4:
        product_categories = st.multiselect(
            "Product Category", base_options["product_category"], default=[], key="trade_filter_category"
        )

    # Product Name phụ thuộc Product Category đã chọn.
    name_options = get_filter_options(df, selected_categories=product_categories)

    with col5:
        product_names = st.multiselect(
            "Product Name", name_options["product_name"], default=[], key="trade_filter_name"
        )
    with col6:
        value_units = st.multiselect(
            "Value Unit", base_options["value_unit"], default=[], key="trade_filter_value_unit"
        )
    with col7:
        quantity_units = st.multiselect(
            "Quantity Unit", base_options["quantity_unit"], default=[], key="trade_filter_quantity_unit"
        )

    st.markdown("</div>", unsafe_allow_html=True)

    return apply_filters(
        df, years, months, trade_types, product_categories, product_names, value_units, quantity_units
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
    """Render 7 KPI cards: Total Trade Value, Total Quantity, Average Unit
    Value, Average MoM Growth, Average YoY Growth, Largest Product Share,
    Top Trading Product.

    Args:
        df: DataFrame đã được lọc theo filter hiện tại.
    """
    st.markdown('<div class="trade-section-title">Tổng quan KPI</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="empty-state">Không có dữ liệu phù hợp với bộ lọc hiện tại.</div>', unsafe_allow_html=True)
        return

    total_trade_value = df["trade_value"].sum()
    total_quantity = df["quantity"].sum()
    avg_unit_value = calculate_average_unit_value(df)
    avg_mom = df["mom_growth_rate"].mean()
    avg_yoy = df["yoy_growth_rate"].mean()
    largest_share = df["product_share_pct"].max()

    top_product = "N/A"
    product_totals = df.groupby("product_name")["trade_value"].sum()
    if not product_totals.empty:
        top_product = str(product_totals.idxmax())

    mom_class = "kpi-positive" if avg_mom >= 0 else "kpi-negative"
    yoy_class = "kpi-positive" if avg_yoy >= 0 else "kpi-negative"

    # Đơn vị giá trị phù hợp cho Average Unit Value: ưu tiên cặp
    # value_unit / quantity_unit phổ biến nhất trong dữ liệu đã lọc.
    unit_label = "N/A"
    if not df.empty:
        value_unit_mode = df["value_unit"].mode()
        quantity_unit_mode = df["quantity_unit"].mode()
        if not value_unit_mode.empty and not quantity_unit_mode.empty:
            unit_label = f"{value_unit_mode.iloc[0]}/{quantity_unit_mode.iloc[0]}"

    avg_unit_value_display = (
        "N/A" if pd.isna(avg_unit_value) else f"{_format_number(avg_unit_value)} {unit_label}"
    )

    cols = st.columns(7)
    kpi_data = [
        ("Total Trade Value", _format_number(total_trade_value), ""),
        ("Total Quantity", _format_number(total_quantity), ""),
        ("Average Unit Value", avg_unit_value_display, ""),
        ("Average MoM Growth", _format_percent(avg_mom), mom_class),
        ("Average YoY Growth", _format_percent(avg_yoy), yoy_class),
        ("Largest Product Share", _format_percent(largest_share), ""),
        ("Top Trading Product", top_product, "kpi-positive"),
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

def chart_trade_value_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart: Trade Value theo Month."""
    grouped = (
        df.groupby(["year", "month"], as_index=False)
        .agg(trade_value=("trade_value", "sum"))
        .sort_values(["year", "month"])
    )
    grouped["month_label"] = grouped["year"].astype(str) + "-" + grouped["month"].astype(str).str.zfill(2)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["month_label"],
            y=grouped["trade_value"],
            mode="lines+markers",
            name="Trade Value",
            line=dict(color=COLOR_ACCENT, width=3),
        )
    )
    fig.update_layout(title="Trade Value Trend")
    return _apply_chart_theme(fig)


def chart_growth_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart 2 đường: MoM Growth & YoY Growth theo Month."""
    grouped = (
        df.groupby(["year", "month"], as_index=False)
        .agg(
            mom_growth_rate=("mom_growth_rate", "mean"),
            yoy_growth_rate=("yoy_growth_rate", "mean"),
        )
        .sort_values(["year", "month"])
    )
    grouped["month_label"] = grouped["year"].astype(str) + "-" + grouped["month"].astype(str).str.zfill(2)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["month_label"],
            y=grouped["mom_growth_rate"],
            mode="lines+markers",
            name="MoM Growth",
            line=dict(color=COLOR_ACCENT, width=2.5),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=grouped["month_label"],
            y=grouped["yoy_growth_rate"],
            mode="lines+markers",
            name="YoY Growth",
            line=dict(color=COLOR_POSITIVE, width=2.5),
        )
    )
    fig.update_layout(title="Growth Trend")
    return _apply_chart_theme(fig)


def chart_import_vs_export(df: pd.DataFrame) -> go.Figure:
    """Vẽ Stacked Area Chart: Trade Value theo Month, group theo Trade Type."""
    grouped = (
        df.groupby(["year", "month", "trade_type"], as_index=False)
        .agg(trade_value=("trade_value", "sum"))
        .sort_values(["year", "month"])
    )
    grouped["month_label"] = grouped["year"].astype(str) + "-" + grouped["month"].astype(str).str.zfill(2)

    fig = px.area(
        grouped,
        x="month_label",
        y="trade_value",
        color="trade_type",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Import vs Export",
        labels={"month_label": "Month", "trade_value": "Trade Value", "trade_type": "Trade Type"},
    )
    return _apply_chart_theme(fig)


def chart_trade_share(df: pd.DataFrame) -> go.Figure:
    """Vẽ Donut Chart: Trade Share theo Trade Type (dựa trên Trade Value)."""
    grouped = df.groupby("trade_type", as_index=False).agg(trade_value=("trade_value", "sum"))

    fig = px.pie(
        grouped,
        names="trade_type",
        values="trade_value",
        hole=0.55,
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Trade Share",
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return _apply_chart_theme(fig)


def chart_top10_trade_value(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 Product theo Trade Value."""
    grouped = (
        df.groupby("product_name", as_index=False)
        .agg(trade_value=("trade_value", "sum"))
        .sort_values("trade_value", ascending=False)
        .head(10)
        .sort_values("trade_value")
    )

    fig = px.bar(
        grouped,
        x="trade_value",
        y="product_name",
        orientation="h",
        color_discrete_sequence=[COLOR_ACCENT],
        title="Top 10 Trade Value",
        labels={"trade_value": "Trade Value", "product_name": "Product"},
    )
    return _apply_chart_theme(fig)


def chart_top10_quantity(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 Product theo Quantity."""
    grouped = (
        df.groupby("product_name", as_index=False)
        .agg(quantity=("quantity", "sum"))
        .sort_values("quantity", ascending=False)
        .head(10)
        .sort_values("quantity")
    )

    fig = px.bar(
        grouped,
        x="quantity",
        y="product_name",
        orientation="h",
        color_discrete_sequence=[COLOR_POSITIVE],
        title="Top 10 Quantity",
        labels={"quantity": "Quantity", "product_name": "Product"},
    )
    return _apply_chart_theme(fig)


def chart_treemap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Treemap: Trade Type -> Product Category -> Product Name.

    Kích thước ô theo Trade Value, màu sắc theo YoY Growth Rate.
    """
    grouped = (
        df.groupby(["trade_type", "product_category", "product_name"], as_index=False)
        .agg(trade_value=("trade_value", "sum"), yoy_growth_rate=("yoy_growth_rate", "mean"))
    )
    grouped = grouped[grouped["trade_value"] > 0]

    fig = px.treemap(
        grouped,
        path=["trade_type", "product_category", "product_name"],
        values="trade_value",
        color="yoy_growth_rate",
        color_continuous_scale=[COLOR_NEGATIVE, "#FFFFFF", COLOR_POSITIVE],
        color_continuous_midpoint=0,
        title="Trade Structure (Treemap)",
        labels={"yoy_growth_rate": "YoY Growth (%)"},
    )
    return _apply_chart_theme(fig, height=480)


def chart_quantity_vs_trade_value(df: pd.DataFrame) -> go.Figure:
    """Vẽ Bubble Scatter: Quantity (X) vs Trade Value (Y), size = Average
    Unit Value, color = Trade Type.
    """
    grouped = (
        df.groupby(["product_name", "trade_type"], as_index=False)
        .agg(quantity=("quantity", "sum"), trade_value=("trade_value", "sum"))
    )
    grouped["avg_unit_value"] = grouped.apply(
        lambda row: row["trade_value"] / row["quantity"] if row["quantity"] not in (0, None) else 0,
        axis=1,
    )
    grouped["bubble_size"] = grouped["avg_unit_value"].abs().fillna(0) + 1

    fig = px.scatter(
        grouped,
        x="quantity",
        y="trade_value",
        size="bubble_size",
        color="trade_type",
        hover_name="product_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Quantity vs Trade Value",
        labels={
            "quantity": "Quantity",
            "trade_value": "Trade Value",
            "trade_type": "Trade Type",
        },
    )
    return _apply_chart_theme(fig)


def chart_growth_heatmap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Heatmap: Product Name (rows) x Month (columns) = YoY Growth Rate."""
    grouped = df.copy()
    grouped["month_label"] = grouped["year"].astype(str) + "-" + grouped["month"].astype(str).str.zfill(2)

    pivot = grouped.pivot_table(
        index="product_name",
        columns="month_label",
        values="yoy_growth_rate",
        aggfunc="mean",
    )

    fig = px.imshow(
        pivot,
        color_continuous_scale=[COLOR_NEGATIVE, "#FFFFFF", COLOR_POSITIVE],
        aspect="auto",
        title="Growth Heatmap (YoY)",
        labels=dict(x="Month", y="Product", color="YoY Growth (%)"),
    )
    return _apply_chart_theme(fig, height=460)


def chart_current_vs_previous(df: pd.DataFrame) -> go.Figure:
    """Vẽ Grouped Bar: Current vs Previous Month vs Previous Year.

    Sử dụng trade_value, trade_value_pre_month, trade_value_pre_year.
    """
    current = df["trade_value"].sum()
    prev_month = df["trade_value_pre_month"].sum()
    prev_year = df["trade_value_pre_year"].sum()

    labels = ["Current", "Previous Month", "Previous Year"]
    values = [current, prev_month, prev_year]
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
    fig.update_layout(title="Current vs Previous Month vs Previous Year")
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
    st.markdown('<div class="trade-chart-wrapper">', unsafe_allow_html=True)
    if df.empty:
        _empty_chart_placeholder()
    else:
        fig = render_fn(df)
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_trade_trend(df: pd.DataFrame) -> None:
    """Render Row 1: Trade Value Trend (Line) & Growth Trend (Line)."""
    st.markdown('<div class="trade-section-title">Xu hướng Kim ngạch & Tăng trưởng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_trade_value_trend, df)
    with col2:
        _chart_card(chart_growth_trend, df)


def render_import_export_section(df: pd.DataFrame) -> None:
    """Render Row 2: Import vs Export (Stacked Area) & Trade Share (Donut)."""
    st.markdown('<div class="trade-section-title">Xuất khẩu & Nhập khẩu</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_import_vs_export, df)
    with col2:
        _chart_card(chart_trade_share, df)


def render_structure_section(df: pd.DataFrame) -> None:
    """Render Row 4: Treemap Trade Structure (Trade Type -> Category -> Product)."""
    st.markdown('<div class="trade-section-title">Cấu trúc Thương mại (Treemap)</div>', unsafe_allow_html=True)
    _chart_card(chart_treemap, df)


def render_ranking_section(df: pd.DataFrame) -> None:
    """Render Row 3: Top 10 Trade Value & Top 10 Quantity (Horizontal Bar)."""
    st.markdown('<div class="trade-section-title">Bảng xếp hạng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_top10_trade_value, df)
    with col2:
        _chart_card(chart_top10_quantity, df)


def render_growth_section(df: pd.DataFrame) -> None:
    """Render Row 5: Quantity vs Trade Value (Bubble Scatter) & Growth Heatmap."""
    st.markdown('<div class="trade-section-title">Phân tích Khối lượng & Tăng trưởng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_quantity_vs_trade_value, df)
    with col2:
        _chart_card(chart_growth_heatmap, df)


def render_comparison_section(df: pd.DataFrame) -> None:
    """Render Row 6: Current vs Previous Month vs Previous Year (Grouped Bar)."""
    st.markdown('<div class="trade-section-title">So sánh Current / Previous Month / Previous Year</div>', unsafe_allow_html=True)
    _chart_card(chart_current_vs_previous, df)


def render_drilldown_section(df: pd.DataFrame) -> None:
    """Render Row 7: Trade Drill-down theo Trade Type -> Product Category.

    Hiển thị bảng chi tiết Product Name gồm Trade Value, Quantity,
    Average Unit Value, MoM, YoY, Share.
    """
    st.markdown('<div class="trade-section-title">Trade Drill-down</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="trade-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    trade_types = sorted(df["trade_type"].dropna().unique().tolist())
    if not trade_types:
        st.markdown('<div class="trade-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.markdown('<div class="trade-card">', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        selected_trade_type = st.selectbox(
            "Chọn Trade Type", trade_types, key="trade_drilldown_trade_type"
        )

    type_df = df[df["trade_type"] == selected_trade_type]
    categories = sorted(type_df["product_category"].dropna().unique().tolist())

    with col2:
        selected_category = st.selectbox(
            "Chọn Product Category", categories if categories else ["N/A"], key="trade_drilldown_category"
        )
    st.markdown("</div>", unsafe_allow_html=True)

    if not categories:
        st.markdown('<div class="trade-chart-wrapper">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    category_df = type_df[type_df["product_category"] == selected_category]

    drilldown_table = (
        category_df.groupby("product_name", as_index=False)
        .agg(
            trade_value=("trade_value", "sum"),
            quantity=("quantity", "sum"),
            mom_growth_rate=("mom_growth_rate", "mean"),
            yoy_growth_rate=("yoy_growth_rate", "mean"),
            product_share_pct=("product_share_pct", "mean"),
        )
    )
    drilldown_table["avg_unit_value"] = drilldown_table.apply(
        lambda row: row["trade_value"] / row["quantity"] if row["quantity"] not in (0, None) else float("nan"),
        axis=1,
    )
    drilldown_table = drilldown_table.sort_values("product_share_pct", ascending=False).rename(
        columns={
            "product_name": "Product Name",
            "trade_value": "Trade Value",
            "quantity": "Quantity",
            "avg_unit_value": "Average Unit Value",
            "mom_growth_rate": "MoM Growth (%)",
            "yoy_growth_rate": "YoY Growth (%)",
            "product_share_pct": "Share (%)",
        }
    )
    drilldown_table = drilldown_table[
        ["Product Name", "Trade Value", "Quantity", "Average Unit Value", "MoM Growth (%)", "YoY Growth (%)", "Share (%)"]
    ]

    with st.expander("Xem chi tiết bảng dữ liệu", expanded=True):
        st.markdown('<div class="trade-chart-wrapper">', unsafe_allow_html=True)
        if drilldown_table.empty:
            _empty_chart_placeholder()
        else:
            st.dataframe(
                drilldown_table.style.format(
                    {
                        "Trade Value": "{:,.2f}",
                        "Quantity": "{:,.2f}",
                        "Average Unit Value": "{:,.2f}",
                        "MoM Growth (%)": "{:.2f}",
                        "YoY Growth (%)": "{:.2f}",
                        "Share (%)": "{:.2f}",
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
    """Render toàn bộ International Trade Performance Dashboard.

    Đây là entry point duy nhất được gọi từ tabs/trade.py. Hàm này
    điều phối toàn bộ flow: inject CSS -> load data -> filter -> KPI
    -> các section biểu đồ -> comparison -> drill-down.
    """
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        st.markdown('<div class="trade-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không thể tải dữ liệu từ gold.fact_international_trade.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    filtered_df = render_filters(raw_df)

    render_kpis(filtered_df)
    render_trade_trend(filtered_df)
    render_import_export_section(filtered_df)
    render_structure_section(filtered_df)
    render_ranking_section(filtered_df)
    render_growth_section(filtered_df)
    render_comparison_section(filtered_df)
    render_drilldown_section(filtered_df)