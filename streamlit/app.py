"""
Entry point chính của ứng dụng Streamlit.

File này CHỈ chịu trách nhiệm:
    - Cấu hình trang (page config).
    - Tạo menu điều hướng (st.tabs).
    - Gọi hàm render() tương ứng của từng dashboard nằm trong shared/.

Không chứa bất kỳ logic xử lý dữ liệu, Spark, hay vẽ biểu đồ nào.
Khi cần thêm dashboard mới, chỉ cần:
    1. Tạo file mới trong shared/ (vd: shared/sales_performance.py) với hàm render_dashboard().
    2. Tạo file mới trong tabs/ (vd: tabs/sales.py) gọi render_dashboard() đó.
    3. Thêm 1 tab mới vào TAB_LABELS bên dưới và import tương ứng.
"""
from shared.gdp_growth import render_dashboard as gdp_render
from shared.crop_yield import render_dashboard as crop_render
from shared.production_output import render_dashboard as product_output_render
from shared.international_trade import render_dashboard as international_trade_render
from shared.social_investment import render_dashboard as social_investment_render
from shared.investment_by_sector import render_dashboard as investment_by_sector_render
import streamlit as st

st.set_page_config(
    page_title="Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ----------------------------------------------------------------
# Danh sách menu (nhãn hiển thị trên tab)
# Thêm dashboard mới bằng cách append label vào đây và xử lý
# tương ứng trong khối if/elif bên dưới.
# ----------------------------------------------------------------
TAB_LABELS = [
    "📈 GDP Dashboard",
    "🌾 Crop Yield Dashboard",
    "Social Investment Source",
    "International Production Trade Dashboard",
    "🏭 National Production Dashboard",
    # "💰 Sales Performance",  # ví dụ: bỏ comment khi thêm dashboard mới
]

tabs = st.tabs(TAB_LABELS)

with tabs[0]:
    gdp_render()  # noqa: F401

with tabs[1]:
    crop_render()  # noqa: F401

with tabs[4]:
    product_output_render()  # noqa: F401

with tabs[3]:
    international_trade_render()
    
with tabs[2]:
    social_investment_render()

# Ví dụ thêm dashboard mới trong tương lai:
# with tabs[3]:
#     from tabs import sales  # noqa: F401