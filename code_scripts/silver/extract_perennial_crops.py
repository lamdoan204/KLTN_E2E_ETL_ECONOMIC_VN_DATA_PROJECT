import re

import pyspark.pandas as pd

from Load_data_to_table import *
from reuse_function import *
from search_sheet_index import search_start_and_end_index, LAUNAM_TITLE


def insert_perennial_crops(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất và insert dữ liệu cây lâu năm."""
    if sheet_index == -1:
        print('Không công bố dữ liệu về cây lâu năm !!!!!!!!!!!!!!')
        return

    caylaunam_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)

    if year < 2025:
        # -- Định dạng cũ (có tách diện tích / sản lượng) --
        start_index, end_index = search_start_and_end_index(LAUNAM_TITLE, caylaunam_sheet)
        caylaunam_sheet = caylaunam_sheet.iloc[start_index:end_index, [0, 2]].reset_index(drop=True)

        start_index_area       = -1
        start_index_production = -1
        col_0 = caylaunam_sheet[0]
        i = 0
        for row in col_0:
            if isinstance(row, str) and 'dientichgieotrong' in clean_text(row): start_index_area = i
            if isinstance(row, str) and 'sanluongnghintan'  in clean_text(row): start_index_production = i
            i += 1

        if start_index_area < start_index_production:
            area_df       = caylaunam_sheet.iloc[start_index_area:start_index_production, ::].reset_index(drop=True)
            production_df = caylaunam_sheet.iloc[start_index_production:len(caylaunam_sheet), ::].reset_index(drop=True)

        area_unit       = re.search(r"\((.*?)\)", area_df.iloc[0, 0])
        production_unit = re.search(r"\((.*?)\)", production_df.iloc[0, 0])

        column_name = ['crop_name', 'value']
        area_df.columns       = column_name
        production_df.columns = column_name

        production_df['unit'] = production_unit.group(1)
        production_df = production_df.iloc[1:]
        area_df['unit'] = area_unit.group(1)
        area_df = area_df.dropna()

        production_df = production_df.rename(columns={'value': 'production', 'unit': 'production_unit'})
        area_df       = area_df.rename(columns={'value': 'area',       'unit': 'area_unit'})

        production_df['crop_name'] = production_df['crop_name'].str.replace(r'\s*\(.*?\)', '', regex=True)

        merged_df = production_df.merge(
            area_df[['crop_name', 'area', 'area_unit']],
            on='crop_name',
            how='inner',
        )

        merged_df['yield']      = merged_df['production'] / merged_df['area'] * 10
        merged_df['yield_unit'] = 'Tạ/ha'

    else:
        # -- Định dạng mới (chỉ có sản lượng) --
        caylaunam_sheet = caylaunam_sheet.iloc[::, [0, 2]]
        column_name = ['crop_name', 'production']
        caylaunam_sheet.columns = column_name
        caylaunam_sheet['production_unit'] = 'Nghìn tấn'
        caylaunam_sheet = caylaunam_sheet.dropna().reset_index(drop=True)
        caylaunam_sheet['yield'], caylaunam_sheet['area'], \
        caylaunam_sheet['yield_unit'], caylaunam_sheet['area_unit'] = None, None, None, None

        merged_df = caylaunam_sheet

    merged_df['year']      = year
    merged_df['ingest_at'] = pd.Timestamp.now()

    insert_df_to_table_silver_layer(merged_df, 'perennial_crops', year, quarter)
