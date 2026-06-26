import re
import pyspark.pandas as pd
import traceback

from Load_data_to_table import *
from reuse_function import *
from search_sheet_index import search_start_and_end_index, LAUNAM_TITLE

def _to_float(series):
    """Chuyển cột giá trị về float, hỗ trợ cả số đã là float/int
    và string dùng dấu phẩy thập phân kiểu VN (vd '123,7')."""
    return (
        series.astype(str)
        .str.strip()
        .str.replace(',', '.', regex=False)
        .replace({'': None, 'nan': None})
        .astype(float)
    )

def insert_perennial_crops(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    try:
        if sheet_index == -1:
            print('Không công bố dữ liệu về cây lâu năm !!!!!!!!!!!!!!')
            return

        caylaunam_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
        if year < 2025:
            # -- Định dạng cũ (< 2025) --
            start_index, end_index = search_start_and_end_index(LAUNAM_TITLE, caylaunam_sheet)
            caylaunam_sheet = caylaunam_sheet.iloc[start_index:end_index, [0, 1, 2]].reset_index(drop=True)
            caylaunam_sheet.columns = range(caylaunam_sheet.shape[1])

            start_index_area       = -1
            start_index_production = -1
            
            # --- BƯỚC KHẮC PHỤC LỖI INDEX 0 OUT OF BOUNDS ---
            # Tự động loại bỏ khoảng trắng để match chính xác mà không cần clean_text()
            for i, val in enumerate(caylaunam_sheet[0]):
                val_str = str(val).lower()
                val_clean = clean_text(val_str)
                if 'diệntích' in val_clean or 'dientich' in val_clean:
                    start_index_area = i
                elif 'sảnlượng' in val_clean or 'sanluong' in val_clean:
                    start_index_production = i

            # Chốt chặn an toàn: Báo lỗi trực tiếp thay vì văng exception khó hiểu
            if start_index_area == -1 or start_index_production == -1:
                print(f"BỎ QUA {year}: Không tìm thấy dòng chứa chữ 'Diện tích' hoặc 'Sản lượng' ở cột 0.")
                return

            if start_index_area < start_index_production:
                area_df       = caylaunam_sheet.iloc[start_index_area:start_index_production, [0, 1, 2]].reset_index(drop=True)
                production_df = caylaunam_sheet.iloc[start_index_production:len(caylaunam_sheet), [0, 1, 2]].reset_index(drop=True)
            else:
                area_df       = caylaunam_sheet.iloc[start_index_area:len(caylaunam_sheet), [0, 1, 2]].reset_index(drop=True)
                production_df = caylaunam_sheet.iloc[start_index_production:start_index_area, [0, 1, 2]].reset_index(drop=True)
            
            # Lấy đơn vị và gán giá trị mặc định nếu biểu thức chính quy (Regex) không khớp
            area_unit_match = re.search(r"\((.*?)\)", str(area_df.iloc[0, 0]))
            area_unit = area_unit_match.group(1) if area_unit_match else 'Nghìn ha'

            production_unit_match = re.search(r"\((.*?)\)", str(production_df.iloc[0, 0]))
            production_unit = production_unit_match.group(1) if production_unit_match else 'Nghìn tấn'

            if year < 2018:
                area_df       = area_df[[1, 2]].reset_index(drop=True)
                production_df = production_df[[1, 2]].reset_index(drop=True)
            else:
                area_df       = area_df[[0, 2]].reset_index(drop=True)
                production_df = production_df[[0, 2]].reset_index(drop=True)

            column_name = ['crop_name', 'value']
            area_df.columns       = column_name
            production_df.columns = column_name

            production_df = production_df.dropna()
            area_df = area_df.dropna()
            production_df['unit'] = production_unit
            production_df = production_df.iloc[1:].dropna(subset=['value']).reset_index(drop=True)

            area_df['unit'] = area_unit
            area_df = area_df.iloc[1:].dropna(subset=['value']).reset_index(drop=True)

            production_df = production_df.rename(columns={'value': 'production', 'unit': 'production_unit'})
            area_df       = area_df.rename(columns={'value': 'area',       'unit': 'area_unit'})

            # Dọn dẹp tên cây trồng
            for df in [production_df, area_df]:
                df['crop_name'] = df['crop_name'].astype(str)
                df['crop_name'] = df['crop_name'].str.replace(r'\s*\(.*?\)', '', regex=True)
                df['crop_name'] = df['crop_name'].str.strip()
                df['crop_name'] = df['crop_name'].str.replace('Chè búp', 'Chè', regex=False)

            production_df['production'] = _to_float(production_df['production'])
            area_df['area']             = _to_float(area_df['area'])

            merged_df = production_df.merge(
                area_df[['crop_name', 'area', 'area_unit']],
                on='crop_name',
                how='inner',
            )

            merged_df['yield']      = merged_df['production'] / merged_df['area'] * 10
            merged_df['yield_unit'] = 'Tạ/ha'

        else:
            # -- Định dạng mới (>= 2025) --
            caylaunam_sheet = caylaunam_sheet.iloc[:, [0, 2]].copy()
            column_name = ['crop_name', 'production']
            caylaunam_sheet.columns = column_name
            
            # Xóa các dòng rỗng
            caylaunam_sheet = caylaunam_sheet.dropna(subset=['production']).reset_index(drop=True)
            caylaunam_sheet['production_unit'] = 'Nghìn tấn'
            caylaunam_sheet = caylaunam_sheet.dropna()
            # Dọn dẹp tên cây trồng
            caylaunam_sheet['crop_name'] = caylaunam_sheet['crop_name'].astype(str)
            caylaunam_sheet['crop_name'] = caylaunam_sheet['crop_name'].str.replace(r'\s*\(.*?\)', '', regex=True)
            caylaunam_sheet['crop_name'] = caylaunam_sheet['crop_name'].str.strip()
            caylaunam_sheet['crop_name'] = caylaunam_sheet['crop_name'].str.replace('Chè búp', 'Chè', regex=False)

            # Ép kiểu dữ liệu
            caylaunam_sheet['production'] = _to_float(caylaunam_sheet['production'])

            # Ép kiểu các cột rỗng để PySpark có thể dự đoán được Schema
            caylaunam_sheet['yield'] = None
            caylaunam_sheet['yield'] = caylaunam_sheet['yield'].astype('float64')

            caylaunam_sheet['area'] = None
            caylaunam_sheet['area'] = caylaunam_sheet['area'].astype('float64')

            caylaunam_sheet['yield_unit'] = None
            caylaunam_sheet['yield_unit'] = caylaunam_sheet['yield_unit'].astype('string')

            caylaunam_sheet['area_unit'] = None
            caylaunam_sheet['area_unit'] = caylaunam_sheet['area_unit'].astype('string')

            # 1. Thay {} (dict rỗng) bằng 'Not Available' trong các cột chứa dict
            for col in ['yield_unit', 'area_unit']:
                caylaunam_sheet[col] = caylaunam_sheet[col].apply(
                    lambda x: 'Not Available' if isinstance(x, dict) else x
                )

            # 2. Thay NaN trong các cột numeric (yield, area) bằng 'Not Available'
            for col in ['yield', 'area']:
                caylaunam_sheet[col] = caylaunam_sheet[col].apply(
                    lambda x: 'Not Available' if pd.isna(x) else x
                )

            # 3. fillna chung cho toàn bộ DataFrame (phòng các cột khác còn NaN)
            caylaunam_sheet = caylaunam_sheet.fillna('Not Available')

            merged_df = caylaunam_sheet
            # 1. Ép các cột cần điền chữ về kiểu dữ liệu object (hỗ trợ cả Số lẫn Chữ)
            for col in ['yield', 'area', 'yield_unit', 'area_unit']:
                merged_df[col] = merged_df[col].astype('object')

            # 2. Định nghĩa các giá trị được coi là trống/lỗi và quy đổi chúng về NaN chuẩn của Pandas
            # (Xử lý các ô chứa dict rỗng {}, chuỗi rỗng '', hoặc chữ 'nan')
            for col in ['yield', 'area', 'yield_unit', 'area_unit']:
                merged_df[col] = merged_df[col].apply(
                    lambda x: None if (isinstance(x, dict) and not x) or str(x).strip().lower() in ['nan', 'none', ''] else x
                )

            # 3. Tiến hành điền 'Not Available' vào tất cả các vị trí NaN/None còn sót lại
            merged_df['yield']      = merged_df['yield'].fillna(-1)
            merged_df['area']       = merged_df['area'].fillna(-1)
            merged_df['yield_unit'] = merged_df['yield_unit'].fillna('Not Available')
            merged_df['area_unit']  = merged_df['area_unit'].fillna('Not Available')

        # Gán thông tin thời gian chung
        merged_df['year']      = year
        merged_df['ingest_at'] = pd.Timestamp.now()
        
        merged_df = merged_df.drop_duplicates()
        merged_df['area'] = merged_df['area'].round(3)
        merged_df['yield'] = merged_df['yield'].round() 
        
        # Đẩy dữ liệu sạch vào Silver layer
        insert_df_to_table_silver_layer(merged_df, 'perennial_crops', year, quarter)

    except Exception as e:
        print('Vấn đề trong trích xuất dữ liệu cây trồng lâu năm: ', e)
        traceback.print_exc()