import pandas as pd
from Load_data_to_table import *
from search_sheet_index import search_start_and_end_index, LAMNGHIEP_TITLE

# Định nghĩa từ điển đơn vị tính chuẩn của Tổng cục Thống kê cho ngành Lâm nghiệp
# Đây là "tấm lưới bảo hiểm" giúp file 2018 không bao giờ bị map sai đơn vị
DEFAULT_FORESTRY_UNITS = {
    'Diện tích rừng trồng mới tập trung': 'Nghìn ha',
    'Diện tích rừng trồng tập trung': 'Nghìn ha',
    'Số cây lâm nghiệp trồng phân tán': 'Triệu cây',
    'Số cây trồng phân tán': 'Triệu cây',
    'Sản lượng gỗ khai thác': 'Nghìn m3',
    'Sản lượng củi khai thác': 'Triệu ste',
    'Chặt phá rừng': 'Ha',
    'Cháy rừng': 'Ha',
    'Diện tích rừng khoanh nuôi tái sinh': 'Nghìn ha',
    'Diện tích rừng được chăm sóc': 'Nghìn ha'
}

def insert_forestry(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất, chuẩn hóa và insert dữ liệu lâm nghiệp (Hỗ trợ file cũ 2018)."""
    
    lamnghiep_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    start_index, end_index = search_start_and_end_index(LAMNGHIEP_TITLE, lamnghiep_sheet)

    # --- SỬA LỖI LỆCH CỘT CỦA FILE 2018 ---
    # Thay vì cố định cột [0, 2], ta lấy cột 0 và cột có chứa dữ liệu số đầu tiên tìm thấy
    sub_df = lamnghiep_sheet.iloc[start_index:end_index]
    
    # Tìm cột chứa giá trị (thường là cột có tỷ lệ dữ liệu số cao nhất, loại trừ cột 0)
    numeric_col = 2  # Mặc định
    for col in sub_df.columns[1:]:
        if pd.to_numeric(sub_df[col], errors='coerce').notna().sum() > 0:
            numeric_col = col
            break

    lamnghiep_sheet = sub_df[[0, numeric_col]].dropna().reset_index(drop=True)
    lamnghiep_sheet.columns = ['forestry_indicator', 'value']

    # Gán thông tin thời gian
    lamnghiep_sheet['quarter']   = quarter
    lamnghiep_sheet['year']      = year
    lamnghiep_sheet['ingest_at'] = pd.Timestamp.now()

    # --- CHUẨN HÓA TEXT ---
    # Tách unit gốc từ ngoặc đơn (nếu có)
    lamnghiep_sheet['unit'] = lamnghiep_sheet['forestry_indicator'].str.extract(r"\((.*?)\)")

    # Làm sạch tên chỉ tiêu
    lamnghiep_sheet['forestry_indicator'] = (
        lamnghiep_sheet['forestry_indicator']
        .str.replace(r'\s*\(.*?\)', '', regex=True)
        .str.strip()
    )
    

    # Đồng bộ tên chỉ tiêu cũ (2018) và mới
    mapping_indicators = {
        'Diện tích rừng trồng tập trung': 'Diện tích rừng trồng mới tập trung',
        'Số cây trồng phân tán': 'Số cây lâm nghiệp trồng phân tán'
    }
    lamnghiep_sheet['forestry_indicator'] = lamnghiep_sheet['forestry_indicator'].replace(mapping_indicators)

    # Xóa dòng tổng thiệt hại
    lamnghiep_sheet = lamnghiep_sheet[lamnghiep_sheet['forestry_indicator'] != 'Diện tích rừng bị thiệt hại']

    # --- SỬA LỖI MAPPING ĐƠN VỊ TÍNH CHO FILE 2018 ---
    # Bước 1: Lấy unit từ file hiện tại (bỏ qua dòng lỗi Quý 1/2020)
    valid_units = lamnghiep_sheet[
        ~((lamnghiep_sheet['year'] == 2020) & (lamnghiep_sheet['quarter'] == 1) & (lamnghiep_sheet['unit'] == 'Ha'))
    ].dropna(subset=['unit'])
    
    dynamic_lookup = valid_units.drop_duplicates(subset=['forestry_indicator']).set_index('forestry_indicator')['unit'].to_dict()

    # Bước 2: Kết hợp bảng tra cứu động và bảng từ điển cứng (Ưu tiên từ điển cứng để an toàn)
    final_lookup = {**dynamic_lookup, **DEFAULT_FORESTRY_UNITS}

    # Bước 3: Áp đơn vị tính chuẩn
    lamnghiep_sheet['unit'] = lamnghiep_sheet['forestry_indicator'].map(final_lookup).fillna('Ha')

    # --- LÀM TRÒN VÀ LỌC DỮ LIỆU SỐ ---
    lamnghiep_sheet['value'] = pd.to_numeric(lamnghiep_sheet['value'], errors='coerce').round(3)
    lamnghiep_sheet = lamnghiep_sheet.dropna(subset=['value'])
    
    lamnghiep_sheet = lamnghiep_sheet.drop_duplicates()

    # Đẩy vào Silver Layer
    insert_df_to_table_silver_layer(lamnghiep_sheet, 'forestry', year, quarter)