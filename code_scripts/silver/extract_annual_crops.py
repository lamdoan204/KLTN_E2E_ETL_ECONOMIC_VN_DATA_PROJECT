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

    # Chuẩn hóa nhãn chỉ tiêu: bỏ khoảng trắng đầu/cuối và phần đơn vị trong "(...)"
    # để so khớp chính xác với metrics_map (vd "    Diện tích (Nghìn ha)" -> "Diện tích")
    metric_label = cayhangnam_sheet['product'].str.strip().str.replace(r"\s*\(.*?\)", "", regex=True)

    # Tách tên cây và chỉ tiêu
    cayhangnam_sheet['crop_name'] = cayhangnam_sheet['product'].where(
        ~metric_label.isin(['Diện tích', 'Năng suất', 'Sản lượng'])
    ).ffill()
    cayhangnam_sheet['unit'] = cayhangnam_sheet['product'].str.extract(r"\((.*?)\)").fillna(' ')

    metrics_map = {
        'Diện tích': 'area',
        'Năng suất': 'yield',
        'Sản lượng': 'production',
    }

    # xác định dòng crop
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

    result['year']      = year
    result['ingest_at'] = pd.Timestamp.now()
    insert_df_to_table_silver_layer(result, 'annual_crops', year, quarter)