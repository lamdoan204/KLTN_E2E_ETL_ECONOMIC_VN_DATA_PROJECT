import pyspark.pandas as pd
import numpy as np
from Load_data_to_table import *
from search_sheet_index import search_start_and_end_index, HANGNAM_TITLE


def insert_annual_crops(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất và insert dữ liệu cây hằng năm."""
    if sheet_index == -1:
        print('Không công bố dữ liệu về cây trồng hằng năm')
        return

    cayhangnam_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    start_index, end_index = search_start_and_end_index(HANGNAM_TITLE, cayhangnam_sheet)
    cayhangnam_sheet = (
        cayhangnam_sheet.iloc[start_index + 1:end_index, [0, 2]]
        .reset_index(drop=True)
    )
    cayhangnam_sheet[2] = cayhangnam_sheet[2].fillna(' ')
    cayhangnam_sheet = cayhangnam_sheet.dropna().reset_index(drop=True)
    column_name = ['product', 'value']
    cayhangnam_sheet.columns = column_name

    # 1. Chuẩn hóa nhãn chỉ tiêu: bỏ khoảng trắng đầu/cuối và phần đơn vị trong "(...)"
    metric_label = cayhangnam_sheet['product'].str.strip().str.replace(r"\s*\(.*?\)", "", regex=True)

    # 2. Tách tên cây thô ban đầu (chưa điền khuyết ffill)
    cayhangnam_sheet['crop_name_raw'] = cayhangnam_sheet['product'].where(
        ~metric_label.isin(['Diện tích', 'Năng suất', 'Sản lượng'])
    ).str.strip()

    # ==================== ĐOẠN ĐỔI TÊN & GỘP CÂY THUỐC ====================
    # Gộp cả "Thuốc lá" và "Thuốc lá, thuốc lào" (sau khi đã strip khoảng trắng) thành "Cây thuốc"
    cayhangnam_sheet['crop_name_raw'] = cayhangnam_sheet['crop_name_raw'].replace(
        ['Thuốc lá', 'Thuốc lá, thuốc lào'], 
        'Cây thuốc'
    )
    # =====================================================================

    # 3. Tiến hành ffill tên cây đã được chuẩn hóa xuống các dòng chỉ tiêu bên dưới
    cayhangnam_sheet['crop_name'] = cayhangnam_sheet['crop_name_raw'].ffill()
    cayhangnam_sheet['unit'] = cayhangnam_sheet['product'].str.extract(r"\((.*?)\)").fillna(' ')

    metrics_map = {
        'Diện tích': 'area',
        'Năng suất': 'yield',
        'Sản lượng': 'production',
    }

    # xác định dòng crop (sử dụng crop_name đã được chuẩn hóa và gộp)
    cayhangnam_sheet['crop_group'] = np.where(
        ~metric_label.isin(metrics_map.keys()),
        cayhangnam_sheet['crop_name'],
        np.nan,
    )
    cayhangnam_sheet['crop_group'] = cayhangnam_sheet['crop_group'].ffill()

    # chỉ giữ metric rows
    detail = cayhangnam_sheet[metric_label.isin(metrics_map.keys())].copy()
    detail['metric'] = metric_label[metric_label.isin(metrics_map.keys())].map(metrics_map)

    # pivot values & units
    values_pivot = detail.pivot(index='crop_group', columns='metric', values='value')
    unit_pivot   = detail.pivot(index='crop_group', columns='metric', values='unit')

    result = pd.DataFrame({
        'crop_name':       values_pivot.index,
        'area':            values_pivot['area'],
        'area_unit':       unit_pivot['area'],
        'yield':           values_pivot['yield'],
        'yield_unit':      unit_pivot['yield'],
        'production':      values_pivot['production'],
        'production_unit': unit_pivot['production'],
    }).reset_index(drop=True)
    
    for col in ['area', 'yield', 'production']:
        result[col] = pd.to_numeric(result[col], errors='coerce').round(3)
    result = result.drop_duplicates()
    result['year']      = year
    result['ingest_at'] = pd.Timestamp.now()
    insert_df_to_table_silver_layer(result, 'annual_crops', year, quarter)