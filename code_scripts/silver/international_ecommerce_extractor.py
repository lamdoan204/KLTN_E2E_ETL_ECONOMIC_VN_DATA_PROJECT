import pyspark.pandas as pd

from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *

# TRÍCH XUẤT DỮ LIỆU THƯƠNG MẠI QUỐC TẾ
def extract_intenational_ecommerce_data_sheet_02(sheet : pd.DataFrame, type : str, month: int, year : int):
    try:
        # xóa các row không cần thiết
        num_of_remove_row = 0
        for i in range(len(sheet)):
            num_of_remove_row += 1
            if isinstance(sheet.iloc[i, 0], str) and 'mathangchuyeu' in clean_text(sheet.iloc[i, 0]): break

        if type == 'Import':
            sheet = sheet.iloc[num_of_remove_row:len(sheet) - 1, ::].reset_index(drop =True)
            
        else: sheet = sheet.iloc[num_of_remove_row::, ::].reset_index(drop =True)

        name_colums = ['product_name', 'quantity', 'value']
        #xoa cac cot kh can thiet
        sheet = sheet.iloc[::, [1, 2, 3]]
        sheet.columns = name_colums

        if type == 'Import': 
            for i in range(len(sheet)):
                if 'oto' == clean_text(str(sheet.loc[i, 'product_name'])):
                    sheet.loc[i, 'product_name'] = 'Ô tô và linh kiện'
                if 'Trong đó: Nguyên chiếc(*)' in sheet.loc[i, 'product_name'] :
                    sheet.loc[ i, 'product_name'] = 'Ô tô nguyên chiếc' 
        # sheet['product_name'] =  sheet['product_name'].str.strip()
        sheet['type'] = type
        quantity_unit = 'Nghìn tấn'
        unit = 'Triệu USD'
        sheet['quantity_unit'] = quantity_unit
        sheet['unit'] = unit
        sheet['month'] = month
        sheet['quarter'] = int((month -1) / 3) + 1
        sheet['year'] = year
        sheet['ingest_at'] = pd.Timestamp.now()
        sheet['quantity'] = sheet['quantity'].fillna(-1)
        sheet.loc[sheet['quantity'] == -1 , 'quantity_unit'] = 'Not Available'  
        sheet = sheet.dropna()
        return sheet
    except Exception as e:
        print(f'CÓ VẤN ĐỀ XẢY RA KHI TRÍCH XUẤT DỮ LIỆU THƯƠNG MẠI QUỐC TÊ NĂM {year}, THÁNG {month}', e)

def extract_intenational_ecommerce_data_sheet_01(sheet : pd.DataFrame, type: str, month: int, year: int):
    try:
    # xóa các row không cần thiết
        num_of_remove_row = 0
        for i in range(len(sheet)):
            num_of_remove_row += 1
            if isinstance(sheet.iloc[i, 0], str) and 'mathangchuyeu' in clean_text(sheet.iloc[i, 0]): break

        if type == 'Import':
            sheet = sheet.iloc[num_of_remove_row:len(sheet) - 1, ::].reset_index(drop =True)
            
        else: sheet = sheet.iloc[num_of_remove_row::, ::].reset_index(drop =True)

        name_colums = ['product_name', 'quantity', 'value']
        # xoa cac cot kh can thiet
        sheet = sheet.iloc[::, [1, 5, 6]]
        sheet.columns = name_colums
        if type == 'import': sheet.loc[ 29, 'product_name'] = 'Ô tô-nguyên chiếc' 
        # sheet['product_name'] = sheet['product_name'].str.strip()
        unit = 'Triệu USD'
        quantity_unit = 'Nghìn tấn'
        ingest_at = pd.Timestamp.now()
        quarter = int((month -1 )/ 3) + 1
        sheet['type'] = type
        sheet['unit'], sheet['quantity_unit'], sheet['month'], sheet['quarter'], sheet['year'], sheet['ingest_at'] = \
            unit, quantity_unit, month, quarter, year, ingest_at
        sheet = sheet.fillna(-1)
        sheet.loc[sheet['quantity'] == -1 , 'quantity_unit'] = 'Not Available'  
        sheet = sheet.dropna()
        return sheet
    except Exception as e:
        print(f'CÓ VẤN ĐỀ XẢY RA KHI TRÍCH XUẤT DỮ LIỆU THƯƠNG MẠI QUỐC TÊ NĂM {year}, THÁNG {month}', e)

def extract_data_from_International_Ecommerce(excel_file: pd.ExcelFile, year, month):
    all_sheets = excel_file.sheet_names
    import_sheet = None
    export_sheet = None
    
    # code xác định sheet báo cáo dữ liệu thương mại quốc tế
    for i in range(len(all_sheets)):
        sheet_name = clean_text(all_sheets[i])
        if any(name in sheet_name for name in ['nk', 'nhapkhau']) and all(name not in sheet_name for name in ['quy', 'gia']):
            import_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
        if any(name in sheet_name for name in ['xuatkhau', 'xk']) and all(name not in sheet_name for name in ['quy', 'gia']):
            export_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
        if import_sheet is not None and export_sheet is not None : break

    if year > 2018 or (year == 2018 and month >= 9) :
        # gọi hàm trích xuất được thiết kế ở trên 
        import_sheet = extract_intenational_ecommerce_data_sheet_02(import_sheet, 'Import', month, year)
        export_sheet = extract_intenational_ecommerce_data_sheet_02(export_sheet, 'Export', month, year)
        # load lên silver với 1 schema nào đó
        insert_df_to_table_silver_layer(import_sheet, 'international_ecommerce', year, month)
        insert_df_to_table_silver_layer(export_sheet, 'international_ecommerce', year, month)
        
    else:
        import_sheet = extract_intenational_ecommerce_data_sheet_01(import_sheet, 'Import', month, year)
        export_sheet = extract_intenational_ecommerce_data_sheet_01(export_sheet, 'Export', month, year)
        # từ sheet trích xuất dữ liệu và load vào silver theo 1 schema nào đó
        insert_df_to_table_silver_layer(import_sheet, 'international_ecommerce', year, month)
        insert_df_to_table_silver_layer(export_sheet, 'international_ecommerce', year, month)
