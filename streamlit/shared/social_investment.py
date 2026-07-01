"""
shared/social_investment.py

National Social Investment Dashboard.

Module này chịu trách nhiệm toàn bộ:
- Truy vấn Spark (Gold layer) và join Dimension.
- Áp dụng filter toàn cục.
- Tính toán KPI và contribution.
- Vẽ toàn bộ dashboard bằng Plotly, đồng bộ giao diện với GDP, Crop,
  Production và International Trade Dashboard.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from shared.spark import get_spark_session

# ============================================================
# THEME / CSS
# ============================================================

COLOR_BACKGROUND = "#081A36"
COLOR_CARD = "#102B55"
COLOR_BORDER = "#2C6FB8"
COLOR_HEADER = "#1B4F9C"
COLOR_ACCENT = "#3FA9F5"
COLOR_POSITIVE = "#2ECC71"
COLOR_NEGATIVE = "#E74C3C"

CHART_COLOR_SEQUENCE: List[str] = [
    "#3FA9F5",
    "#2ECC71",
    "#E74C3C",
    "#F5A623",
    "#9B59B6",
    "#1ABC9C",
    "#E67E22",
    "#5DADE2",
]


def _inject_css() -> None:
    """Inject CSS để đồng bộ giao diện (background, card, border, header) với các dashboard khác."""
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
        }}
        .investment-header {{
            background-color: {COLOR_HEADER};
            border-radius: 12px;
            padding: 20px 26px;
            margin-bottom: 18px;
            box-shadow: 0 4px 14px rgba(0,0,0,0.25);
            border: 1px solid {COLOR_BORDER};
        }}
        .investment-header h1 {{
            color: #FFFFFF;
            font-size: 26px;
            font-weight: 700;
            margin: 0;
        }}
        .investment-header p {{
            color: #CFE3FB;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}
        .inv-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 16px 18px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.22);
            margin-bottom: 16px;
        }}
        .inv-card h3 {{
            color: #FFFFFF;
            font-size: 16px;
            font-weight: 600;
            margin: 0 0 12px 0;
            border-left: 4px solid {COLOR_ACCENT};
            padding-left: 10px;
        }}
        [data-testid="stMetric"] {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 12px;
            padding: 14px 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }}
        [data-testid="stMetricLabel"] {{
            color: #CFE3FB !important;
        }}
        [data-testid="stMetricValue"] {{
            color: #FFFFFF !important;
        }}
        [data-testid="stMetricDelta"] {{
            color: #CFE3FB !important;
        }}
        .stTabs [data-baseweb="tab-list"] {{
            gap: 6px;
        }}
        .stTabs [data-baseweb="tab"] {{
            background-color: {COLOR_CARD};
            border-radius: 8px 8px 0 0;
            color: #CFE3FB;
            border: 1px solid {COLOR_BORDER};
        }}
        .stTabs [aria-selected="true"] {{
            background-color: {COLOR_HEADER};
            color: #FFFFFF;
        }}
        section[data-testid="stSidebar"] {{
            background-color: {COLOR_CARD};
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _section_title(title: str) -> None:
    """Mở thẻ div card và render tiêu đề cho một section chart."""
    st.markdown(f"<div class='inv-card'><h3>{title}</h3>", unsafe_allow_html=True)


def _section_end() -> None:
    """Đóng thẻ div card của section."""
    st.markdown("</div>", unsafe_allow_html=True)


def _apply_chart_layout(fig: go.Figure, height: int = 380) -> go.Figure:
    """
    Áp dụng layout chuẩn cho mọi biểu đồ Plotly: nền trắng, font, spacing,
    legend ngang phía trên bên phải.

    Args:
        fig: Plotly Figure cần style.
        height: chiều cao chart tính bằng pixel.

    Returns:
        go.Figure đã được style.
    """
    fig.update_layout(
        height=height,
        paper_bgcolor="#140F4E",
        plot_bgcolor="#140F4E",
        font=dict(color="#1B1B1B", size=12),
        margin=dict(l=30, r=30, t=40, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


# ============================================================
# DATA LOADING
# ============================================================

@st.cache_resource(show_spinner="Đang tải dữ liệu Social Investment...")
def load_data() -> SparkDataFrame:
    """
    Đọc dữ liệu từ Gold layer và join fact_social_total_investment với
    dim_time và dim_capital_source.

    Sử dụng SparkSession có sẵn từ shared/spark.py, không tạo session mới.

    Returns:
        SparkDataFrame: dữ liệu đã join, chưa áp dụng filter.
    """
    spark = get_spark_session()

    fact_df = spark.table("gold.fact_social_total_investment")
    dim_time_df = spark.table("gold.dim_time")
    dim_source_df = spark.table("gold.dim_capital_source")

    joined_df = (
        fact_df
        .join(dim_time_df, on="time_key", how="inner")
        .join(dim_source_df, on="capital_source_key", how="inner")
        .select(
            dim_time_df["year"],
            dim_time_df["quarter"],
            dim_source_df["source_name"],
            fact_df["unit"],
            fact_df["investment_value"],
            fact_df["investment_value_pre_quarter"],
            fact_df["investment_value_pre_year"],
            fact_df["qoq_growth_rate"],
            fact_df["yoy_growth_rate"],
            fact_df["source_share_pct"],
        )
    )
    return joined_df


def get_filter_options(spark_df: SparkDataFrame) -> Dict[str, List[Any]]:
    """
    Lấy danh sách giá trị duy nhất cho từng filter trực tiếp từ Spark DataFrame.

    Args:
        spark_df: Spark DataFrame đã join (chưa filter).

    Returns:
        Dict gồm 'years', 'quarters', 'sources', 'units'.
    """
    years = [
        row["year"] for row in
        spark_df.select("year").distinct().orderBy("year").collect()
    ]
    quarters = [
        row["quarter"] for row in
        spark_df.select("quarter").distinct().orderBy("quarter").collect()
    ]
    sources = [
        row["source_name"] for row in
        spark_df.select("source_name").distinct().orderBy("source_name").collect()
    ]
    units = [
        row["unit"] for row in
        spark_df.select("unit").distinct().orderBy("unit").collect()
    ]

    return {
        "years": years,
        "quarters": quarters,
        "sources": sources,
        "units": units,
    }


@st.cache_data(show_spinner="Đang áp dụng bộ lọc...")
def apply_filters(
    _spark_df: SparkDataFrame,
    years: Tuple[int, ...],
    quarters: Tuple[int, ...],
    sources: Tuple[str, ...],
    units: Tuple[str, ...],
) -> pd.DataFrame:
    """
    Áp dụng filter bằng Spark DataFrame API, chỉ convert sang Pandas sau khi
    đã filter xong (chuẩn bị cho bước vẽ Plotly).

    Args:
        _spark_df: Spark DataFrame nguồn (prefix "_" để Streamlit không hash).
        years: danh sách năm được chọn.
        quarters: danh sách quý được chọn.
        sources: danh sách nguồn vốn được chọn.
        units: danh sách đơn vị được chọn.

    Returns:
        pandas.DataFrame đã filter, có thêm cột quarter_label.
    """
    filtered_df = _spark_df

    if years:
        filtered_df = filtered_df.filter(F.col("year").isin(list(years)))
    if quarters:
        filtered_df = filtered_df.filter(F.col("quarter").isin(list(quarters)))
    if sources:
        filtered_df = filtered_df.filter(F.col("source_name").isin(list(sources)))
    if units:
        filtered_df = filtered_df.filter(F.col("unit").isin(list(units)))

    pandas_df = filtered_df.toPandas()

    if not pandas_df.empty:
        pandas_df["quarter_label"] = (
            pandas_df["year"].astype(str) + "-Q" + pandas_df["quarter"].astype(str)
        )
        pandas_df = pandas_df.sort_values(["year", "quarter"]).reset_index(drop=True)

    return pandas_df


@st.cache_data(show_spinner=False)
def calculate_contribution(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Tính mức đóng góp QoQ của từng nguồn vốn vào tổng vốn đầu tư của quý
    gần nhất trong dữ liệu đã filter.

    Args:
        pdf: pandas DataFrame đã filter.

    Returns:
        pandas.DataFrame gồm 'source_name' và 'qoq_contribution', sắp xếp
        giảm dần theo mức đóng góp.
    """
    if pdf.empty:
        return pd.DataFrame(columns=["source_name", "qoq_contribution"])

    latest_year = pdf["year"].max()
    latest_quarter = pdf.loc[pdf["year"] == latest_year, "quarter"].max()

    latest_df = pdf[
        (pdf["year"] == latest_year) & (pdf["quarter"] == latest_quarter)
    ].copy()

    latest_df["qoq_contribution"] = (
        latest_df["investment_value"] - latest_df["investment_value_pre_quarter"]
    )

    result = (
        latest_df[["source_name", "qoq_contribution"]]
        .sort_values("qoq_contribution", ascending=False)
        .reset_index(drop=True)
    )

    return result


# ============================================================
# FILTER UI
# ============================================================

def render_filters(spark_df: SparkDataFrame) -> Dict[str, Tuple[Any, ...]]:
    """
    Render bộ lọc toàn cục: Year, Quarter, Capital Source, Unit.

    Args:
        spark_df: Spark DataFrame nguồn dùng để lấy danh sách filter.

    Returns:
        Dict chứa các giá trị filter đã chọn (dạng tuple để có thể cache).
    """
    options = get_filter_options(spark_df)

    st.markdown("<div class='inv-card'>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        selected_years = st.multiselect(
            "Year", options=options["years"], default=options["years"], key="inv_year"
        )
    with col2:
        selected_quarters = st.multiselect(
            "Quarter", options=options["quarters"], default=options["quarters"], key="inv_quarter"
        )
    with col3:
        selected_sources = st.multiselect(
            "Capital Source", options=options["sources"], default=options["sources"], key="inv_source"
        )
    with col4:
        selected_units = st.multiselect(
            "Unit", options=options["units"], default=options["units"], key="inv_unit"
        )

    st.markdown("</div>", unsafe_allow_html=True)

    return {
        "years": tuple(selected_years),
        "quarters": tuple(selected_quarters),
        "sources": tuple(selected_sources),
        "units": tuple(selected_units),
    }


# ============================================================
# KPI
# ============================================================

def render_kpis(pdf: pd.DataFrame) -> None:
    """
    Render 6 KPI Card: Total Investment, Avg QoQ Growth, Avg YoY Growth,
    Largest Capital Source, Largest Source Share, Number of Capital Sources.

    Args:
        pdf: pandas DataFrame đã filter.
    """
    if pdf.empty:
        st.warning("Không có dữ liệu phù hợp với bộ lọc hiện tại.")
        return

    total_investment = pdf["investment_value"].sum()
    avg_qoq = pdf["qoq_growth_rate"].mean()
    avg_yoy = pdf["yoy_growth_rate"].mean()

    by_source = pdf.groupby("source_name")["investment_value"].sum()
    largest_source = by_source.idxmax()
    largest_share = pdf["source_share_pct"].max()
    num_sources = pdf["source_name"].nunique()

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Total Investment", f"{total_investment:,.0f}")
    with col2:
        st.metric("Avg QoQ Growth", f"{avg_qoq:.2f}%")
    with col3:
        st.metric("Avg YoY Growth", f"{avg_yoy:.2f}%")
    with col4:
        st.metric("Largest Capital Source", f"{largest_source}")
    with col5:
        st.metric("Largest Source Share", f"{largest_share:.2f}%")
    with col6:
        st.metric("Number of Capital Sources", f"{num_sources}")


# ============================================================
# TREND SECTION (ROW 1)
# ============================================================

def render_trend_section(pdf: pd.DataFrame) -> None:
    """
    Render Row 1: Investment Trend (Line) và Growth Trend (Line - QoQ & YoY).

    Args:
        pdf: pandas DataFrame đã filter.
    """
    if pdf.empty:
        return

    col1, col2 = st.columns(2)

    with col1:
        _section_title("Investment Trend")
        trend_df = pdf.groupby("quarter_label", as_index=False)["investment_value"].sum()
        fig = px.line(
            trend_df,
            x="quarter_label",
            y="investment_value",
            markers=True,
            color_discrete_sequence=[COLOR_ACCENT],
        )
        fig.update_layout(xaxis_title="Quarter", yaxis_title="Investment Value")
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()

    with col2:
        _section_title("Growth Trend")
        growth_df = pdf.groupby("quarter_label", as_index=False).agg(
            qoq_growth_rate=("qoq_growth_rate", "mean"),
            yoy_growth_rate=("yoy_growth_rate", "mean"),
        )
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=growth_df["quarter_label"],
                y=growth_df["qoq_growth_rate"],
                mode="lines+markers",
                name="QoQ Growth",
                line=dict(color=COLOR_ACCENT, width=2),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=growth_df["quarter_label"],
                y=growth_df["yoy_growth_rate"],
                mode="lines+markers",
                name="YoY Growth",
                line=dict(color=COLOR_POSITIVE, width=2),
            )
        )
        fig.update_layout(xaxis_title="Quarter", yaxis_title="Growth Rate (%)")
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()


# ============================================================
# STRUCTURE SECTION (ROW 2 + ROW 4)
# ============================================================

def render_structure_section(pdf: pd.DataFrame) -> None:
    """
    Render Row 2 (Stacked Bar, Donut) và Row 4 (Treemap, 100% Stacked Area)
    thể hiện cơ cấu nguồn vốn đầu tư.

    Args:
        pdf: pandas DataFrame đã filter.
    """
    if pdf.empty:
        return

    col1, col2 = st.columns(2)

    with col1:
        _section_title("Investment by Capital Source")
        bar_df = pdf.groupby(
            ["quarter_label", "source_name"], as_index=False
        )["investment_value"].sum()
        fig = px.bar(
            bar_df,
            x="quarter_label",
            y="investment_value",
            color="source_name",
            color_discrete_sequence=CHART_COLOR_SEQUENCE,
        )
        fig.update_layout(
            xaxis_title="Quarter", yaxis_title="Investment Value", barmode="stack"
        )
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()

    with col2:
        _section_title("Capital Source Share")
        share_df = pdf.groupby("source_name", as_index=False)["investment_value"].sum()
        fig = px.pie(
            share_df,
            names="source_name",
            values="investment_value",
            hole=0.55,
            color_discrete_sequence=CHART_COLOR_SEQUENCE,
        )
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()

    col3, col4 = st.columns(2)

    with col3:
        _section_title("Treemap")
        tree_df = pdf.groupby("source_name", as_index=False).agg(
            investment_value=("investment_value", "sum"),
            yoy_growth_rate=("yoy_growth_rate", "mean"),
        )
        fig = px.treemap(
            tree_df,
            path=["source_name"],
            values="investment_value",
            color="yoy_growth_rate",
            color_continuous_scale=["#E74C3C", "#F5A623", "#2ECC71"],
        )
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()

    with col4:
        _section_title("Capital Source Structure")
        area_df = pdf.groupby(
            ["quarter_label", "source_name"], as_index=False
        )["investment_value"].sum()
        fig = px.area(
            area_df,
            x="quarter_label",
            y="investment_value",
            color="source_name",
            groupnorm="fraction",
            color_discrete_sequence=CHART_COLOR_SEQUENCE,
        )
        fig.update_layout(xaxis_title="Quarter", yaxis_title="Share")
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()


# ============================================================
# RANKING SECTION (ROW 3)
# ============================================================

def render_ranking_section(pdf: pd.DataFrame) -> None:
    """
    Render Row 3: Top Investment Sources và Top Growth Sources
    (Horizontal Bar).

    Args:
        pdf: pandas DataFrame đã filter.
    """
    if pdf.empty:
        return

    col1, col2 = st.columns(2)

    with col1:
        _section_title("Top Investment Sources")
        top_inv = pdf.groupby("source_name", as_index=False)["investment_value"].sum()
        top_inv = top_inv.sort_values("investment_value", ascending=True)
        fig = px.bar(
            top_inv,
            x="investment_value",
            y="source_name",
            orientation="h",
            color_discrete_sequence=[COLOR_ACCENT],
        )
        fig.update_layout(xaxis_title="Investment Value", yaxis_title="")
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()

    with col2:
        _section_title("Top Growth Sources")
        top_growth = pdf.groupby("source_name", as_index=False)["yoy_growth_rate"].mean()
        top_growth = top_growth.sort_values("yoy_growth_rate", ascending=True)
        bar_colors = [
            COLOR_POSITIVE if value >= 0 else COLOR_NEGATIVE
            for value in top_growth["yoy_growth_rate"]
        ]
        fig = go.Figure(
            go.Bar(
                x=top_growth["yoy_growth_rate"],
                y=top_growth["source_name"],
                orientation="h",
                marker_color=bar_colors,
            )
        )
        fig.update_layout(xaxis_title="YoY Growth Rate (%)", yaxis_title="")
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()


# ============================================================
# GROWTH SECTION (ROW 5)
# ============================================================

def render_growth_section(pdf: pd.DataFrame) -> None:
    """
    Render Row 5: QoQ vs YoY (Bubble Scatter) và Growth Heatmap.

    Args:
        pdf: pandas DataFrame đã filter.
    """
    if pdf.empty:
        return

    col1, col2 = st.columns(2)

    with col1:
        _section_title("QoQ vs YoY")
        bubble_df = pdf.groupby("source_name", as_index=False).agg(
            qoq_growth_rate=("qoq_growth_rate", "mean"),
            yoy_growth_rate=("yoy_growth_rate", "mean"),
            investment_value=("investment_value", "sum"),
        )
        fig = px.scatter(
            bubble_df,
            x="qoq_growth_rate",
            y="yoy_growth_rate",
            size="investment_value",
            color="source_name",
            color_discrete_sequence=CHART_COLOR_SEQUENCE,
            size_max=50,
        )
        fig.update_layout(xaxis_title="QoQ Growth (%)", yaxis_title="YoY Growth (%)")
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()

    with col2:
        _section_title("Growth Heatmap")
        heat_df = pdf.pivot_table(
            index="source_name",
            columns="quarter_label",
            values="yoy_growth_rate",
            aggfunc="mean",
        )
        fig = px.imshow(
            heat_df,
            color_continuous_scale=["#E74C3C", "#F5A623", "#2ECC71"],
            aspect="auto",
        )
        fig.update_layout(xaxis_title="Quarter", yaxis_title="Capital Source")
        st.plotly_chart(_apply_chart_layout(fig), use_container_width=True)
        _section_end()


# ============================================================
# COMPARISON SECTION (ROW 6)
# ============================================================

def render_comparison_section(pdf: pd.DataFrame) -> None:
    """
    Render Row 6: Current vs Previous Quarter vs Previous Year (Grouped Bar).

    Args:
        pdf: pandas DataFrame đã filter.
    """
    if pdf.empty:
        return

    _section_title("Current vs Previous Quarter vs Previous Year")

    comp_df = pdf.groupby("source_name", as_index=False).agg(
        investment_value=("investment_value", "sum"),
        investment_value_pre_quarter=("investment_value_pre_quarter", "sum"),
        investment_value_pre_year=("investment_value_pre_year", "sum"),
    )

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=comp_df["source_name"],
            y=comp_df["investment_value"],
            name="Current",
            marker_color=COLOR_ACCENT,
        )
    )
    fig.add_trace(
        go.Bar(
            x=comp_df["source_name"],
            y=comp_df["investment_value_pre_quarter"],
            name="Previous Quarter",
            marker_color=COLOR_HEADER,
        )
    )
    fig.add_trace(
        go.Bar(
            x=comp_df["source_name"],
            y=comp_df["investment_value_pre_year"],
            name="Previous Year",
            marker_color=COLOR_BORDER,
        )
    )
    fig.update_layout(
        barmode="group", xaxis_title="Capital Source", yaxis_title="Investment Value"
    )
    st.plotly_chart(_apply_chart_layout(fig, height=420), use_container_width=True)
    _section_end()


# ============================================================
# CONTRIBUTION SECTION (ROW 7)
# ============================================================

def render_contribution_section(pdf: pd.DataFrame) -> None:
    """
    Render Row 7: Contribution Waterfall, thể hiện mức đóng góp QoQ của
    từng nguồn vốn vào tổng vốn đầu tư quý gần nhất.

    Args:
        pdf: pandas DataFrame đã filter.
    """
    if pdf.empty:
        return

    contribution_df = calculate_contribution(pdf)
    if contribution_df.empty:
        return

    _section_title("Contribution Waterfall")

    fig = go.Figure(
        go.Waterfall(
            x=contribution_df["source_name"],
            y=contribution_df["qoq_contribution"],
            measure=["relative"] * len(contribution_df),
            increasing=dict(marker=dict(color=COLOR_POSITIVE)),
            decreasing=dict(marker=dict(color=COLOR_NEGATIVE)),
            totals=dict(marker=dict(color=COLOR_ACCENT)),
            connector=dict(line=dict(color=COLOR_BORDER)),
        )
    )
    fig.update_layout(xaxis_title="Capital Source", yaxis_title="QoQ Contribution")
    st.plotly_chart(_apply_chart_layout(fig, height=420), use_container_width=True)
    _section_end()


# ============================================================
# MAIN DASHBOARD
# ============================================================

def render_dashboard() -> None:
    """
    Hàm chính render toàn bộ National Social Investment Dashboard:
    Header, Filters, KPI, Trend, Structure, Ranking, Growth, Comparison,
    Contribution.
    """
    _inject_css()

    st.markdown(
        """
        <div class="investment-header">
            <h1>National Social Investment Dashboard</h1>
            <p>Phân tích vốn đầu tư thực hiện toàn xã hội theo nguồn vốn và theo quý</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    spark_df = load_data()
    filters = render_filters(spark_df)

    pdf = apply_filters(
        spark_df,
        filters["years"],
        filters["quarters"],
        filters["sources"],
        filters["units"],
    )

    if pdf.empty:
        st.warning("Không có dữ liệu phù hợp với bộ lọc hiện tại.")
        return

    render_kpis(pdf)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    render_trend_section(pdf)
    render_structure_section(pdf)
    render_ranking_section(pdf)
    render_growth_section(pdf)
    render_comparison_section(pdf)
    render_contribution_section(pdf)