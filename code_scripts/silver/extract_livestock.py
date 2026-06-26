import pyspark.pandas as pd
from Load_data_to_table import *

# Giả định bạn đã import hàm này
# from Load_data_to_table import insert_df_to_table_silver_layer

def insert_livestock(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Trích xuất, tách đơn vị, chuẩn hóa nhóm và lọc dữ liệu chăn nuôi."""
    channuoi_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    channuoi_sheet = channuoi_sheet.iloc[::, [0, 2]]

    column_name = ['livestock_indicator', 'value']
    channuoi_sheet.columns = column_name

    # =========================================================================
    # FIX LỖI: Ép kiểu dữ liệu về chuỗi (string) để dùng được hàm .str
    # =========================================================================
    channuoi_sheet['livestock_indicator'] = channuoi_sheet['livestock_indicator'].astype("string")

    # =========================================================================
    # BƯỚC 1: TÁCH ĐƠN VỊ VÀ ĐIỀN KHUYẾT (Làm trước khi tên bị chuẩn hóa)
    # =========================================================================
    # Áp dụng ffill() giúp form 2025 nhận được đơn vị từ dòng tiêu đề tổng ở trên
    channuoi_sheet['unit'] = channuoi_sheet['livestock_indicator'].str.extract(r"\((.*?)\)").ffill()

    # =========================================================================
    # BƯỚC 2: CHUẨN HÓA VỀ CHUNG 1 TÊN GỌI (Standardization)
    # =========================================================================
    # Chuyển tạm cột text về chữ thường để dễ bắt từ khóa
    channuoi_sheet['temp_lower'] = channuoi_sheet['livestock_indicator'].str.lower()

    # Quy hoạch lại tên dựa theo các từ khóa đặc trưng
    channuoi_sheet.loc[channuoi_sheet['temp_lower'].str.contains('trứng', na=False), 'livestock_indicator'] = 'Trứng'
    channuoi_sheet.loc[channuoi_sheet['temp_lower'].str.contains('sữa', na=False), 'livestock_indicator'] = 'Sữa'
    channuoi_sheet.loc[channuoi_sheet['temp_lower'].str.contains('lợn|heo', na=False), 'livestock_indicator'] = 'Thịt lợn'
    channuoi_sheet.loc[channuoi_sheet['temp_lower'].str.contains('trâu', na=False), 'livestock_indicator'] = 'Thịt trâu'
    
    # Cẩn thận tránh ghi đè nhầm: Chỉ đổi thành Thịt bò/gia cầm nếu trước đó chưa đổi thành Trứng/Sữa
    is_not_sua = channuoi_sheet['livestock_indicator'] != 'Sữa'
    channuoi_sheet.loc[channuoi_sheet['temp_lower'].str.contains('bò', na=False) & is_not_sua, 'livestock_indicator'] = 'Thịt bò'
    
    is_not_trung = channuoi_sheet['livestock_indicator'] != 'Trứng'
    channuoi_sheet.loc[channuoi_sheet['temp_lower'].str.contains('gia cầm', na=False) & is_not_trung, 'livestock_indicator'] = 'Thịt gia cầm'

    # Xóa cột tạm
    channuoi_sheet = channuoi_sheet.drop(columns=['temp_lower'])

    # =========================================================================
    # BƯỚC 3: LỌC QUA DANH SÁCH VALID & XÓA NAN
    # =========================================================================
    valid_indicators = ['Thịt lợn', 'Thịt gia cầm', 'Thịt trâu', 'Thịt bò', 'Trứng', 'Sữa']
    
    # Chỉ giữ lại các dòng nằm trong danh sách chuẩn hóa
    channuoi_sheet = channuoi_sheet[channuoi_sheet['livestock_indicator'].isin(valid_indicators)]

    # Xóa các dòng bị NaN ban đầu
    channuoi_sheet = channuoi_sheet.dropna(subset=['livestock_indicator', 'value'])
    
    # Ép giá trị về kiểu số (những text rác còn sót lại ở cột value sẽ biến thành NaN)
    channuoi_sheet['value'] = pd.to_numeric(channuoi_sheet['value'], errors='coerce')
    
    # Xóa dòng NaN thêm lần nữa sau khi ép kiểu và làm tròn
    channuoi_sheet = channuoi_sheet.dropna(subset=['value'])
    channuoi_sheet['value'] = channuoi_sheet['value'].round(2)

    # =========================================================================
    # BƯỚC 4: THÊM METADATA VÀ INSERT
    # =========================================================================
    channuoi_sheet['quarter']   = quarter
    channuoi_sheet['year']      = year
    channuoi_sheet['ingest_at'] = pd.Timestamp.now()

    # Loại bỏ lặp dữ liệu trước khi lưu
    channuoi_sheet = channuoi_sheet.drop_duplicates(subset=['livestock_indicator', 'quarter', 'year'], keep='last')
    channuoi_sheet = channuoi_sheet.reset_index(drop=True)
    
    # FIX LỖI: Ép kiểu unit về string trước khi lọc để tránh lỗi nếu có giá trị Null
    channuoi_sheet = channuoi_sheet[channuoi_sheet['unit'].astype("string").str.lower() != 'mủ khô']
    
    channuoi_sheet.loc[channuoi_sheet['livestock_indicator'] == 'Sữa', 'unit'] = 'Triệu lít'
    channuoi_sheet.loc[channuoi_sheet['livestock_indicator'] == 'Trứng', 'unit'] = 'Triệu quả'
    
    channuoi_sheet = channuoi_sheet.drop_duplicates()
    # Gán đơn vị 'Nghìn tấn' cho TẤT CẢ các loại còn lại (không phải Sữa và Trứng)
    channuoi_sheet.loc[~channuoi_sheet['livestock_indicator'].isin(['Sữa', 'Trứng']), 'unit'] = 'Nghìn tấn'
    # Gọi hàm để load lên bảng
    insert_df_to_table_silver_layer(channuoi_sheet, 'livestock', year, quarter)