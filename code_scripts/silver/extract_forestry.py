import pyspark.pandas as pd

from Load_data_to_table import *
from search_sheet_index import search_start_and_end_index, LAMNGHIEP_TITLE


def insert_forestry(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất và insert dữ liệu lâm nghiệp."""
    lamnghiep_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    start_index, end_index = search_start_and_end_index(LAMNGHIEP_TITLE, lamnghiep_sheet)

    lamnghiep_sheet = (
        lamnghiep_sheet.iloc[start_index:end_index, [0, 2]]
        .dropna()
        .reset_index(drop=True)
    )

    column_name = ['forestry_indicator', 'value']
    lamnghiep_sheet.columns = column_name

    lamnghiep_sheet['unit'] = lamnghiep_sheet['forestry_indicator'].str.extract(r"\((.*?)\)")
    lamnghiep_sheet = lamnghiep_sheet.fillna('Ha')
    lamnghiep_sheet['forestry_indicator'] = (
        lamnghiep_sheet['forestry_indicator']
        .str.replace(r'\s*\(.*?\)', '', regex=True)
        .str.strip()
    )

    lamnghiep_sheet['quarter']   = quarter
    lamnghiep_sheet['year']      = year
    lamnghiep_sheet['ingest_at'] = pd.Timestamp.now()

    insert_df_to_table_silver_layer(lamnghiep_sheet, 'forestry', year, quarter)
