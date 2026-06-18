import pyspark.pandas as pd
import numpy as np

from Load_data_to_table import *
from reuse_function import *
from search_sheet_index import search_start_and_end_index, CHUYEU_TITLE


def insert_staple_crops(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất và insert dữ liệu cây trồng chủ yếu."""
    if sheet_index == -1:
        print('Không công bố dữ liệu về cây trồng chủ yếu')
        return

    caychuyeu_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    start_index, end_index = search_start_and_end_index(CHUYEU_TITLE, caychuyeu_sheet)

    caychuyeu_df = (
        caychuyeu_sheet.iloc[start_index + 1:end_index, [0, 1, 3]]
        .reset_index(drop=True)
    )

    # Tìm ranh giới cây có hạt / cây có củ
    start_index_1 = -1  # cây lương thực có hạt
    end_index_1   = -1
    start_index_2 = -1  # cây chất bột có củ

    col_0 = caychuyeu_df[0]
    for i, row in enumerate(col_0):
        if isinstance(row, str) and 'cohat' in clean_text(row):
            start_index_1 = i
        if isinstance(caychuyeu_df.iloc[i, 1], str) and 'tongsanluong' in clean_text(caychuyeu_df.iloc[i, 1]):
            end_index_1 = i - 1
        if isinstance(row, str) and 'cocu' in clean_text(row) and start_index_1 != -1:
            start_index_2 = i

    column_name = ['product_and_infor', f'value_{year}']
    cohat_df = caychuyeu_df.iloc[start_index_1:end_index_1, 1:].dropna(subset=[1]).reset_index(drop=True)
    cocu_df  = caychuyeu_df.iloc[start_index_2:len(caychuyeu_df), 1:].dropna(subset=[1]).reset_index(drop=True)
    cohat_df.columns = column_name
    cocu_df.columns  = column_name

    cohat_df['unit'] = cohat_df['product_and_infor'].str.extract(r"\((.*?)\)").fillna(' ')
    cocu_df['unit']  = cohat_df['product_and_infor'].str.extract(r"\((.*?)\)").fillna(' ')

    def parse_crop_table(df, type_label):
        df = df.copy()
        value_col = f'value_{year}'
        df['crop_name'] = df['product_and_infor'].where(df[value_col].isna()).ffill()
        df = df[df[value_col].notna()].copy()

        df['metric'] = np.select(
            [
                df['product_and_infor'].str.contains('Diện tích'),
                df['product_and_infor'].str.contains('Năng suất'),
                df['product_and_infor'].str.contains('Sản lượng'),
            ],
            ['area', 'yield', 'production'],
            default='other',
        )

        values = df.pivot_table(index='crop_name', columns='metric', values=value_col,   aggfunc='first')
        units  = df.pivot_table(index='crop_name', columns='metric', values='unit',       aggfunc='first')
        units.columns = [f"{c}_unit" for c in units.columns]

        result = pd.concat([values, units], axis=1)
        result = result.rename(columns={
            'area':            'area',
            'yield':           'yield',
            'production':      'production',
            'area_unit':       'area_unit',
            'yield_unit':      'yield_unit',
            'production_unit': 'production_unit',
        })
        result['type'] = type_label
        result = result.reset_index()
        return result[['crop_name', 'type', 'area', 'area_unit', 'yield', 'yield_unit', 'production', 'production_unit']]

    merged_df = pd.concat(
        [
            parse_crop_table(cocu_df,  'cây có củ'),
            parse_crop_table(cohat_df, 'cây có hạt'),
        ],
        ignore_index=True,
    )
    merged_df['year']      = year
    merged_df['ingest_at'] = pd.Timestamp.now()

    insert_df_to_table_silver_layer(merged_df, 'staple_crops', year, quarter, year, quarter)
