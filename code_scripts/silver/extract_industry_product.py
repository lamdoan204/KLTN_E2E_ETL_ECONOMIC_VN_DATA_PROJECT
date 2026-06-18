import pyspark.pandas as pd

from Load_data_to_table import *


def extract_industry_product(sheet: pd.read_excel, month: int, year: int):
    """Trích xuất dữ liệu sản phẩm ngành công nghiệp từ sheet."""
    column_names = ['product_name', 'unit', 'value']
    sheet = sheet.iloc[::, [0, 1, 3]]
    sheet.columns = column_names

    sheet = sheet.dropna().reset_index(drop=True)

    for row_index in range(len(sheet)):
        unit = sheet.loc[row_index, 'unit']
        if '\n' in unit:
            unit = unit.replace('\n', '')
            sheet.loc[row_index, 'unit'] = unit
        if unit == '"':
            pre_unit = sheet.loc[row_index - 1, 'unit']
            sheet.loc[row_index, 'unit'] = pre_unit

    sheet['product_name'] = sheet['product_name'].str.replace('\n', ' ').str.strip()
    sheet['month']     = month
    sheet['quarter']   = int(int((month - 1) / 3) + 1)
    sheet['year']      = year
    sheet['ingest_at'] = pd.Timestamp.now()
    return sheet


def insert_industry_product(excel_file, all_sheets, sheet_index: int, month: int, year: int):
    """Đọc sheet, trích xuất và insert dữ liệu sản phẩm công nghiệp."""
    quarter = int((month - 1) / 3) + 1
    df = extract_industry_product(
        pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None),
        month=month,
        year=year,
    )
    insert_df_to_table_silver_layer(df, 'industry_product', year, quarter)
