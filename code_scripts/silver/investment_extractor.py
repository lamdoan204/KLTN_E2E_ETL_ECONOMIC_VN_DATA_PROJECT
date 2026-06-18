import pyspark.pandas as pd

from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *

# TRÍCH XUẤT DỮ LIỆU ĐẦU TƯ KINH TẾ -  VỐN ĐẦU TƯ TOÀN XÃ HỘI
def extract_data_from_Invesment(excel_file: pd.ExcelFile, year, month):
    try:
        if month % 3 != 0 : return
        quarter = int((month - 1) / 3)  + 1 
        all_sheets = excel_file.sheet_names
        # xác định sheet chứa dữ liệu VDTTXH
        vdt_sheet = None
        for sheet_name in all_sheets:
            sheet = pd.read_excel(excel_file, sheet_name=sheet_name, header= None)
            text_title_cleaned = clean_text(sheet.iloc[0, 0]) if isinstance(sheet.iloc[0,0], str) else ''
            if text_title_cleaned != '' and all(title in text_title_cleaned for title in ['vondautu', 'toanxahoi']):
                vdt_sheet = sheet
                break
                
        if(vdt_sheet is None):
            print(f"KHONG TIM THAY SHEET BAO CAO VDTTXH TRONG EXCEL FILE: year_{2024}, month_{12} !!!!!!!!!")
            return
        # trích xuất dữ liệu
        # lấy các cột càn thiết
        column_names = ['investment_name', 'value']
        vdt_sheet = vdt_sheet.iloc[::, [1, 3]]
        vdt_sheet.columns = column_names
        # xóa các hàng không cần thiết

        num_of_removed_row = -1
        col = vdt_sheet['value']
        for i in range(len(col)):
            num_of_removed_row += 1
            if type(col[i]) is str:
                break
        vdt_sheet = vdt_sheet.iloc[num_of_removed_row::, ::].reset_index(drop= True)

        num_of_removed_row = -1
        for i in range(len(vdt_sheet['investment_name'])):
            num_of_removed_row += 1
            if type(vdt_sheet['investment_name'][i]) is str:
                break

        num_of_removed_row
        vdt_sheet = vdt_sheet.iloc[num_of_removed_row::, ::].reset_index(drop= True)
        # load lên silver layer với 1 schema nào đó
        unit = 'Nghìn tỷ đồng'
        vdt_sheet['unit'] = unit
        vdt_sheet['investment_name'] = vdt_sheet['investment_name'].str.replace('\n', ' ').str.strip()
        vdt_sheet['year'] = year
        vdt_sheet['quarter'] = quarter
        vdt_sheet['ingest_at'] = pd.Timestamp.now()
        vdt_sheet = vdt_sheet.dropna()
        insert_df_to_table_silver_layer(vdt_sheet, 'investment', year, quarter)
    except Exception as e:
        print(f'CÓ VẤN ĐỀ XẢY RA KHI TRÍCH XUẤT DỮ LIỆU VỐN ĐẦU TƯ TOÀN XÃ HỘI NĂM {year}, THÁNG {month}', e)
    # kiểm tra quý nào thiếu thì trích từ file báo cáo excel của quý sau
