"""
Gold Layer Analytics — Streamlit Dashboard
Đọc dữ liệu CHỈ từ database `gold` trong Hive Metastore (Thrift),
dữ liệu thực lưu trên MinIO (S3A), compute qua Spark (standalone cluster).

Kiến trúc:
    Streamlit (container này)
        -> PySpark client (spark.master = spark://spark-master:7077)
        -> Hive Metastore (thrift://hive:9083)  -> chỉ truy cập database `gold`
        -> MinIO (S3A)                          -> nơi thực sự lưu data (parquet/delta...)

Thay đổi so với bản trước:
    - Khoá cứng database = "gold". Không cho chọn database khác qua UI.
    - Tab SQL tuỳ chỉnh: chỉ chấp nhận câu lệnh SELECT, và luôn chạy trong
      context `gold` (không thể FROM database khác).
    - Giao diện thiết kế lại toàn bộ: theme tối, KPI cards, status strip,
      typography riêng cho số liệu (monospace) và nội dung (sans-serif).
"""

import os
import re
import time
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive:9083")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

APP_NAME = os.getenv("SPARK_APP_NAME", "gold-layer-dashboard")

# Database duy nhất mà dashboard này được phép đọc. Khoá cứng có chủ đích —
# đây là ranh giới truy cập ở tầng UI, không phải tầng bảo mật thật (quyền
# thật vẫn nên được set ở Hive/Ranger/IAM phía sau).
GOLD_DATABASE = os.getenv("GOLD_DATABASE", "gold")

st.set_page_config(
    page_title="Gold Layer Analytics",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ----------------------------------------------------------------------------
# STYLE — theme tối, accent vàng-đồng, monospace cho số liệu
# ----------------------------------------------------------------------------
CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');

:root {
    --bg-primary: #0B1320;
    --bg-card: #121C2E;
    --bg-card-hover: #16223A;
    --border-color: #1F2D45;
    --accent-gold: #D4A24C;
    --accent-gold-soft: rgba(212, 162, 76, 0.12);
    --text-primary: #E8ECF2;
    --text-secondary: #7C8AA3;
    --green: #4ADE80;
    --red: #F87171;
}

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, sans-serif;
}

.stApp {
    background-color: var(--bg-primary);
}

/* Ẩn header mặc định của Streamlit để dùng header riêng */
header[data-testid="stHeader"] {
    background: transparent;
}

/* ---- Sidebar ---- */
section[data-testid="stSidebar"] {
    background-color: var(--bg-card);
    border-right: 1px solid var(--border-color);
}

section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stSlider label {
    color: var(--text-secondary) !important;
    font-size: 0.78rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

/* ---- Custom header bar ---- */
.app-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.4rem 0 1.4rem 0;
    border-bottom: 1px solid var(--border-color);
    margin-bottom: 1.6rem;
}
.app-header .title-block h1 {
    font-size: 1.55rem;
    font-weight: 700;
    color: var(--text-primary);
    margin: 0;
    letter-spacing: -0.01em;
}
.app-header .title-block p {
    color: var(--text-secondary);
    font-size: 0.85rem;
    margin: 0.15rem 0 0 0;
}
.gold-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: var(--accent-gold-soft);
    border: 1px solid rgba(212, 162, 76, 0.35);
    color: var(--accent-gold);
    padding: 0.3rem 0.75rem;
    border-radius: 100px;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.03em;
}

/* ---- Status strip (signature element) ---- */
.status-strip {
    display: flex;
    gap: 0.6rem;
    margin-bottom: 1.4rem;
    flex-wrap: wrap;
}
.status-pill {
    display: flex;
    align-items: center;
    gap: 0.45rem;
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 8px;
    padding: 0.45rem 0.9rem;
    font-size: 0.78rem;
    color: var(--text-secondary);
    font-family: 'JetBrains Mono', monospace;
}
.status-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    flex-shrink: 0;
}
.dot-ok { background: var(--green); box-shadow: 0 0 6px var(--green); }
.dot-fail { background: var(--red); box-shadow: 0 0 6px var(--red); }

/* ---- KPI cards ---- */
.kpi-card {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 1.1rem 1.3rem;
    height: 100%;
}
.kpi-label {
    color: var(--text-secondary);
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 0.4rem;
}
.kpi-value {
    font-family: 'JetBrains Mono', monospace;
    color: var(--text-primary);
    font-size: 1.65rem;
    font-weight: 700;
    line-height: 1.1;
}
.kpi-sub {
    color: var(--accent-gold);
    font-size: 0.78rem;
    margin-top: 0.3rem;
    font-family: 'JetBrains Mono', monospace;
}

/* ---- Tabs ---- */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    border-bottom: 1px solid var(--border-color);
}
.stTabs [data-baseweb="tab"] {
    color: var(--text-secondary);
    font-weight: 600;
    font-size: 0.88rem;
    padding: 0.6rem 1rem;
}
.stTabs [aria-selected="true"] {
    color: var(--accent-gold) !important;
    border-bottom: 2px solid var(--accent-gold) !important;
}

/* ---- Dataframe / widgets dùng monospace cho số ---- */
[data-testid="stDataFrame"] {
    font-family: 'JetBrains Mono', monospace;
}

.stButton button {
    background: var(--accent-gold);
    color: #1A1304;
    border: none;
    font-weight: 600;
    border-radius: 8px;
}
.stButton button:hover {
    background: #E0B66A;
    color: #1A1304;
}

[data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', monospace;
}

/* Caption / hint text */
.hint-text {
    color: var(--text-secondary);
    font-size: 0.82rem;
}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ----------------------------------------------------------------------------
# SPARK SESSION
# ----------------------------------------------------------------------------
@st.cache_resource(show_spinner="Đang kết nối tới Spark cluster...")
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


# ----------------------------------------------------------------------------
# GUARD: chỉ cho phép SELECT, và chỉ trong context GOLD_DATABASE
# ----------------------------------------------------------------------------
_SELECT_ONLY_RE = re.compile(r"^\s*(WITH\b.*?\)\s*)?SELECT\b", re.IGNORECASE | re.DOTALL)
_FORBIDDEN_KEYWORDS = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE",
    "MERGE", "REPLACE", "GRANT", "REVOKE", "SET", "USE", "VACUUM", "MSCK",
)


def validate_select_only(sql: str) -> None:
    """Raise ValueError nếu câu SQL không phải SELECT đơn thuần (read-only)."""
    stripped = sql.strip().rstrip(";")
    if not _SELECT_ONLY_RE.match(stripped):
        raise ValueError(
            "Chỉ cho phép câu lệnh SELECT (có thể kèm WITH ... ở đầu). "
            "Không được chạy INSERT/UPDATE/DELETE/DDL từ dashboard."
        )
    upper_sql = stripped.upper()
    for kw in _FORBIDDEN_KEYWORDS:
        # Khớp từ khoá đứng riêng (word boundary) để tránh chặn nhầm tên cột.
        if re.search(rf"\b{kw}\b", upper_sql):
            raise ValueError(f"Câu lệnh chứa từ khoá không được phép: `{kw}`.")


# ----------------------------------------------------------------------------
# DATA ACCESS — luôn ép buộc database = gold
# ----------------------------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=300)
def check_gold_database_exists() -> bool:
    spark = get_spark_session()
    rows = spark.sql("SHOW DATABASES").collect()
    existing = {r[0].lower() for r in rows}
    return GOLD_DATABASE.lower() in existing


@st.cache_data(show_spinner=False, ttl=300)
def list_gold_tables() -> list[str]:
    spark = get_spark_session()
    rows = spark.sql(f"SHOW TABLES IN `{GOLD_DATABASE}`").collect()
    return sorted([r["tableName"] for r in rows])


@st.cache_data(show_spinner=False, ttl=300)
def describe_table(table: str) -> pd.DataFrame:
    spark = get_spark_session()
    rows = spark.sql(f"DESCRIBE TABLE `{GOLD_DATABASE}`.`{table}`").collect()
    return pd.DataFrame(rows, columns=rows[0].asDict().keys() if rows else ["col_name", "data_type", "comment"])


@st.cache_data(show_spinner="Đang query dữ liệu từ gold layer...", ttl=120)
def query_table(table: str, limit: int) -> pd.DataFrame:
    spark = get_spark_session()
    df = spark.sql(f"SELECT * FROM `{GOLD_DATABASE}`.`{table}` LIMIT {limit}")
    return df.toPandas()


@st.cache_data(show_spinner=False, ttl=120)
def get_row_count(table: str) -> int:
    spark = get_spark_session()
    return spark.sql(f"SELECT COUNT(*) AS c FROM `{GOLD_DATABASE}`.`{table}`").collect()[0]["c"]


@st.cache_data(show_spinner=False, ttl=120)
def run_readonly_sql(sql: str) -> pd.DataFrame:
    validate_select_only(sql)
    spark = get_spark_session()
    spark.catalog.setCurrentDatabase(GOLD_DATABASE)
    df = spark.sql(sql)
    return df.toPandas()


# ----------------------------------------------------------------------------
# HEADER + STATUS STRIP
# ----------------------------------------------------------------------------
def render_header():
    st.markdown(
        f"""
        <div class="app-header">
            <div class="title-block">
                <h1>Gold Layer Analytics</h1>
                <p>Curated, business-ready tables · Spark + Hive Metastore + MinIO</p>
            </div>
            <div class="gold-badge">◆ database: {GOLD_DATABASE}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status_strip(spark_ok: bool, gold_ok: bool):
    def pill(label: str, ok: bool):
        dot_class = "dot-ok" if ok else "dot-fail"
        state = "connected" if ok else "unavailable"
        return f"""<div class="status-pill"><span class="status-dot {dot_class}"></span>{label}: {state}</div>"""

    st.markdown(
        f"""
        <div class="status-strip">
            {pill("spark master", spark_ok)}
            {pill("hive metastore", spark_ok)}
            {pill(f"db · {GOLD_DATABASE}", gold_ok)}
        </div>
        """,
        unsafe_allow_html=True,
    )


render_header()

# ----------------------------------------------------------------------------
# CONNECTION CHECK
# ----------------------------------------------------------------------------
spark_ok = True
gold_ok = False
try:
    gold_ok = check_gold_database_exists()
except Exception as e:
    spark_ok = False
    render_status_strip(spark_ok, gold_ok)
    st.error(
        "Không kết nối được tới Spark / Hive Metastore.\n\n"
        "Kiểm tra spark-master, hive, minio đã chạy (`docker compose ps`) "
        f"và network giữa các container.\n\nChi tiết lỗi: `{e}`"
    )
    st.stop()

render_status_strip(spark_ok, gold_ok)

if not gold_ok:
    st.error(
        f"Database `{GOLD_DATABASE}` không tồn tại trong Hive Metastore. "
        "Dashboard này chỉ được phép đọc từ gold layer — kiểm tra lại pipeline "
        "ETL đã chạy đến bước tạo database này chưa, hoặc set biến môi trường "
        "`GOLD_DATABASE` đúng tên thực tế."
    )
    st.stop()

# ----------------------------------------------------------------------------
# SIDEBAR — chỉ chọn table trong gold (không chọn database)
# ----------------------------------------------------------------------------
st.sidebar.markdown("### Nguồn dữ liệu")
st.sidebar.caption(f"`{SPARK_MASTER_URL}`")
st.sidebar.caption(f"`{HIVE_METASTORE_URI}`")
st.sidebar.divider()

try:
    tables = list_gold_tables()
except AnalysisException as e:
    st.sidebar.error(f"Lỗi khi lấy danh sách bảng: {e}")
    st.stop()

if not tables:
    st.warning(f"Database `{GOLD_DATABASE}` chưa có bảng nào. Kiểm tra lại job ETL gold layer.")
    st.stop()

selected_table = st.sidebar.selectbox("Table (gold)", tables)
row_limit = st.sidebar.slider("Số dòng tối đa lấy về", 100, 20_000, 1_000, step=100)

st.sidebar.divider()
if st.sidebar.button("↻ Reload dữ liệu", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown(
    """<p class="hint-text">Dashboard chỉ đọc (read-only) từ layer gold.
    Tab SQL tuỳ chỉnh chỉ chấp nhận câu lệnh SELECT.</p>""",
    unsafe_allow_html=True,
)

# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
try:
    df = query_table(selected_table, row_limit)
    total_rows = get_row_count(selected_table)
except Exception as e:
    st.error(f"Lỗi khi query bảng `{selected_table}`: {e}")
    st.stop()

st.markdown(f"#### `{GOLD_DATABASE}.{selected_table}`")

# ---- KPI cards ----
mem_mb = df.memory_usage(deep=True).sum() / 1e6
k1, k2, k3, k4 = st.columns(4)
kpi_specs = [
    (k1, "Tổng số dòng (bảng)", f"{total_rows:,}", None),
    (k2, "Số dòng đã tải", f"{len(df):,}", f"giới hạn: {row_limit:,}"),
    (k3, "Số cột", f"{df.shape[1]}", None),
    (k4, "Bộ nhớ (ước tính)", f"{mem_mb:.1f} MB", None),
]
for col, label, value, sub in kpi_specs:
    with col:
        sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
        st.markdown(
            f"""<div class="kpi-card">
                    <div class="kpi-label">{label}</div>
                    <div class="kpi-value">{value}</div>
                    {sub_html}
                </div>""",
            unsafe_allow_html=True,
        )

st.write("")
tab_data, tab_chart, tab_schema, tab_sql = st.tabs(
    ["Dữ liệu", "Biểu đồ", "Schema", "SQL (read-only)"]
)

with tab_data:
    st.dataframe(df, use_container_width=True, height=460)
    st.download_button(
        "Tải xuống CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{GOLD_DATABASE}_{selected_table}.csv",
        mime="text/csv",
    )

with tab_chart:
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    all_cols = df.columns.tolist()

    if not numeric_cols:
        st.info("Không có cột số nào để vẽ biểu đồ.")
    else:
        c1, c2, c3 = st.columns(3)
        x_axis = c1.selectbox("Trục X", all_cols)
        y_axis = c2.selectbox("Trục Y (số)", numeric_cols)
        chart_type = c3.selectbox("Loại biểu đồ", ["Line", "Bar", "Scatter", "Area"])

        plot_df = df.copy()
        # Chỉ thử convert datetime nếu cột X không phải kiểu số — tránh việc
        # cột số nguyên (id, year...) bị hiểu nhầm thành epoch timestamp.
        if plot_df[x_axis].dtype == object:
            try:
                plot_df[x_axis] = pd.to_datetime(plot_df[x_axis])
                plot_df = plot_df.sort_values(x_axis)
            except (ValueError, TypeError):
                pass

        chart_fn = {"Line": px.line, "Bar": px.bar, "Scatter": px.scatter, "Area": px.area}[chart_type]

        fig = chart_fn(plot_df, x=x_axis, y=y_axis, title=f"{y_axis} theo {x_axis}")
        fig.update_layout(
            plot_bgcolor="#121C2E",
            paper_bgcolor="#121C2E",
            font_color="#E8ECF2",
            title_font_color="#E8ECF2",
            xaxis=dict(gridcolor="#1F2D45", zerolinecolor="#1F2D45"),
            yaxis=dict(gridcolor="#1F2D45", zerolinecolor="#1F2D45"),
            margin=dict(t=50, l=10, r=10, b=10),
        )
        fig.update_traces(marker_color="#D4A24C") if chart_type in ("Bar", "Scatter") else fig.update_traces(line_color="#D4A24C")
        st.plotly_chart(fig, use_container_width=True)

with tab_schema:
    try:
        schema_df = describe_table(selected_table)
        st.dataframe(schema_df, use_container_width=True, height=420)
    except Exception as e:
        st.error(f"Không lấy được schema: {e}")

with tab_sql:
    st.markdown(
        f'<p class="hint-text">Chạy SQL read-only trong context <code>{GOLD_DATABASE}</code>. '
        "Chỉ câu lệnh SELECT được chấp nhận — INSERT/UPDATE/DELETE/DDL sẽ bị từ chối.</p>",
        unsafe_allow_html=True,
    )
    default_sql = f"SELECT * FROM `{GOLD_DATABASE}`.`{selected_table}` LIMIT 100"
    custom_sql = st.text_area("SQL", value=default_sql, height=120)

    if st.button("▶ Chạy SQL"):
        try:
            t0 = time.time()
            result_df = run_readonly_sql(custom_sql)
            elapsed = time.time() - t0
            st.success(f"Trả về {len(result_df):,} dòng trong {elapsed:.2f}s.")
            st.dataframe(result_df, use_container_width=True)
        except ValueError as ve:
            st.error(f"Bị chặn: {ve}")
        except Exception as e:
            st.error(f"Lỗi SQL: {e}")