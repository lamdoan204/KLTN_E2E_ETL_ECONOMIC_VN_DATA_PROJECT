import pyspark.pandas as pd
import numpy as np

from Load_data_to_table import *
from search_sheet_index import search_start_and_end_index, THUYSAN_TITLE


def insert_aquatic_products(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất và insert dữ liệu thủy sản."""
    thuysan_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    unit = 'Nghìn tấn'

    if thuysan_sheet.shape[1] < 5:
        # -- Định dạng ít cột --
        start_index, end_index = search_start_and_end_index(THUYSAN_TITLE, thuysan_sheet)

        thuysan_sheet = (
            thuysan_sheet.iloc[start_index:end_index, [0, 2]]
            .dropna()
            .reset_index(drop=True)
        )
        thuysan_sheet.columns = ['aquatic_type', 'value']
        thuysan_sheet['unit'] = unit

        idx = thuysan_sheet[thuysan_sheet['aquatic_type'].str.strip() == 'Nuôi trồng'].index[0]
        thuysan_sheet = thuysan_sheet.loc[idx:].reset_index(drop=True)

        # đánh dấu header group
        thuysan_sheet['aquatic_group'] = np.where(
            thuysan_sheet['aquatic_type'].isin(['Nuôi trồng', 'Khai thác']),
            thuysan_sheet['aquatic_type'],
            np.nan,
        )
        thuysan_sheet['aquatic_group'] = thuysan_sheet['aquatic_group'].ffill()

        # loại bỏ dòng tổng
        result = thuysan_sheet[
            ~thuysan_sheet['aquatic_type'].isin(['Nuôi trồng', 'Khai thác'])
        ].copy()

        result['product_name'] = result['aquatic_type']
        result['aquatic_type'] = result['aquatic_group']

        thuysan_sheet = result[['aquatic_type', 'product_name', 'value', 'unit']]

    else:
        # -- Định dạng nhiều cột --
        start_index, end_index = search_start_and_end_index(THUYSAN_TITLE, thuysan_sheet)

        df = thuysan_sheet.iloc[start_index:end_index].copy()
        print(df.shape)
        print(df.columns)
        print(df.head())

        df = df[[1, 2, 4]].copy()
        df.columns = ['aquatic_group', 'product_name', 'value']

        df['aquatic_group'] = df['aquatic_group'].ffill()
        df = df.dropna(subset=['product_name'])

        idx = df[df['aquatic_group'].str.strip() == 'Nuôi trồng'].index[0]
        df = df.loc[idx:].reset_index(drop=True)

        result = df[~df['product_name'].isin(['Nuôi trồng', 'Khai thác'])].copy()
        result['unit'] = unit

        thuysan_sheet = result[['aquatic_group', 'product_name', 'value', 'unit']].rename(
            columns={'aquatic_group': 'aquatic_type'}
        )

    thuysan_sheet['quarter']   = quarter
    thuysan_sheet['year']      = year
    thuysan_sheet['ingest_at'] = pd.Timestamp.now()

    insert_df_to_table_silver_layer(thuysan_sheet, 'aquatic_products', year, quarter)
