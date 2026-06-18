import pyspark.pandas as pd

from Load_data_to_table import *


def insert_livestock(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất và insert dữ liệu chăn nuôi."""
    channuoi_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    channuoi_sheet = channuoi_sheet.iloc[::, [0, 2]]

    column_name = ['livestock_indicator', 'value']
    channuoi_sheet.columns = column_name

    channuoi_sheet['unit'] = channuoi_sheet['livestock_indicator'].str.extract(r"\((.*?)\)").ffill()
    channuoi_sheet['livestock_indicator'] = (
        channuoi_sheet['livestock_indicator']
        .str.replace(r'\s*\(.*?\)', '', regex=True)
        .str.strip()
    )

    channuoi_sheet = channuoi_sheet.dropna().reset_index(drop=True)
    channuoi_sheet['quarter']   = quarter
    channuoi_sheet['year']      = year
    channuoi_sheet['ingest_at'] = pd.Timestamp.now()

    insert_df_to_table_silver_layer(channuoi_sheet, 'livestock', year, quarter)
