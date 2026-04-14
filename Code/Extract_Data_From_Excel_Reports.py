import pandas as pd
from minio_funcs import *
from reuse_function import *


def extract_data_from_GDP(excel_file: pd.ExcelFile, year, month):
    # Kiểm tra phải báo cáo của quý không
    if month % 3 == 0:
        quarter = int( month / 3 )
        all_sheets = excel_file.sheet_names

        if quarter == 1 or (year <= 2018 and quarter <= 3): 
            gdp_sheet = None
            for i in range(len(all_sheets)):
                if 'gdp' in str.lower(all_sheets[i]):
                    gdp_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[i], header= None)

            if gdp_sheet is None:
                print(f"Không xác định được sheet báo cáo GDP trong file báo cáo năm: {year}, tháng: {month} ===========")      
                return
            # đặt lại tên cho cột - bỏ những cột không cần thiết
            column_names = ['sector_and_sub_sectors', 'curent_value', 'comparative_value']
            gdp_sheet = gdp_sheet.iloc[::, [1,2,5]]
            gdp_sheet.columns = column_names
            # xóa các row thừa
            num_of_row_del = 0
            sector_column = gdp_sheet['sector_and_sub_sectors']
            for row in sector_column:
                if isinstance(row, str): break
                num_of_row_del += 1
            gdp_sheet = gdp_sheet.iloc[num_of_row_del::, ::].reset_index(drop= True)
            
            # code load lên silver theo schema nào đó

        elif year >= 2019:
           
            gdp_hh_sheet = None
            gdp_ss_sheet = None
            
            for i in range(len(all_sheets)):
                current_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
                col_0 = current_sheet.iloc[::, 0]

                for row_i in range(len(col_0)):
                    if isinstance(col_0[row_i], str) and 'tongsanphamtrongnuoctheogiahienhanh' in clean_text(col_0[row_i]):
                        if gdp_hh_sheet is None: gdp_hh_sheet = current_sheet
                    if isinstance(col_0[row_i], str) and 'tongsanphamtrongnuoctheogiasosanh' in clean_text(col_0[row_i]):
                            if gdp_ss_sheet is None: gdp_ss_sheet = current_sheet
                    if gdp_hh_sheet != None and gdp_ss_sheet != None: break
                
                if gdp_ss_sheet is None or gdp_ss_sheet is None:
                    print(f"Không xác định được sheet báo cáo GDP trong file báo cáo năm: {year}, tháng: {month} =========")       
                    return None

            # trích xuất dữ liệu
            # lấy đơn vị 
            unit = None
            for i in range(len(gdp_hh_sheet)):
                if isinstance(gdp_hh_sheet.iloc[i, 7], str): unit = gdp_hh_sheet.iloc[i, 7]
            # lấy các cột cần thiết
            gdp_hh_sheet = gdp_hh_sheet.iloc[::, [1,2,3]]
            gdp_ss_sheet = gdp_ss_sheet.iloc[::,[1,2,3]]
            # đặt lại tên cho các cột
            column_names = ['sector_and_sub_sectors', f'quarter_{quarter - 1}', f'quarter_{quarter}']
            gdp_hh_sheet.columns = column_names
            gdp_ss_sheet.columns = column_names
            # xóa các hàng không cần thiết
            num_of_row_del = 0
            sector_column = gdp_hh_sheet['sector_and_sub_sectors']
            for row in sector_column:
                if isinstance(row, str): break
                num_of_row_del += 1
            gdp_hh_sheet = gdp_hh_sheet.iloc[num_of_row_del::, ::].reset_index(drop= True)
            num_of_row_del = 0
            sector_column = gdp_ss_sheet['sector_and_sub_sectors']
            for row in sector_column:
                if isinstance(row, str): break
                num_of_row_del += 1
            gdp_ss_sheet = gdp_ss_sheet.iloc[num_of_row_del::, ::].reset_index(drop= True)
            
            # code load lên silver theo schema nào đó

             
            
        else: # trích xuất dữ liệu trước 2018 quý 4
            gdp_hh_sheet = None
            gdp_ss_sheet = None
            
            for i in range(len(all_sheets)):
                current_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
                col_0 = current_sheet.iloc[::, 0]

                for row_i in range(len(col_0)):
                    if isinstance(col_0[row_i], str) and 'tongsanphamtrongnuoctheogiahienhanh' in clean_text(col_0[row_i]):
                        if gdp_hh_sheet is None: gdp_hh_sheet = current_sheet
                    if isinstance(col_0[row_i], str) and 'tongsanphamtrongnuoctheogiasosanh' in clean_text(col_0[row_i]):
                            if gdp_ss_sheet is None: gdp_ss_sheet = current_sheet

                    if gdp_hh_sheet != None and gdp_ss_sheet != None: break
                
            if gdp_ss_sheet is None or gdp_ss_sheet is None:
                print(f"Không xác định được sheet báo cáo GDP trong file báo cáo năm: {year}, tháng: {month} =========")       
                return 
            # Clean Data
            # lấy những cột cần thiết
            gdp_hh_sheet = gdp_hh_sheet.iloc[::, [0,1,2,3]]
            gdp_ss_sheet - gdp_ss_sheet.iloc[::, [0, 1, 2, 3]]
            # xóa các hàng thừa
            num_of_remove_row_hh = 0
            num_of_remove_row_ss = 0
            col_hh = gdp_hh_sheet[0]
            col_ss = gdp_ss_sheet[0]

            for i in range(len(col_hh)):
                num_of_remove_row_hh += 1
                if isinstance(col_hh[i], str) and 'tongso' == clean_text(col_hh[i]):
                    break
            for i in range(len(col_ss)):
                num_of_remove_row_ss += 1
                if isinstance(col_ss[i], str) and 'tongso' == clean_text(col_ss[i]):
                    break
            gdp_hh_sheet = gdp_hh_sheet.iloc[num_of_remove_row_hh::, ::].reset_index(drop= True)
            gdp_ss_sheet = gdp_ss_sheet.iloc[num_of_remove_row_ss:: , ::]. reset_index(drop= True)

            column_names = ['sector', 'sector_and_sub_sectors', f'year_{year - 1}', f'year_{year}']
            gdp_hh_sheet.columns = column_names
            gdp_ss_sheet.columns = column_names

            for row_index in range(len(gdp_hh_sheet)):
                if type(gdp_hh_sheet.iloc[row_index, 'sector_and_sub_sectors']) is float:
                    gdp_hh_sheet.iloc[row_index, 'sector_and_sub_sectors'] = gdp_hh_sheet.iloc[row_i, 'sector']
                if type(gdp_ss_sheet.iloc['sector_and_sub_sectors']) is float:
                    gdp_ss_sheet.iloc[row_index, 'sector_and_sub_sectors'] = gdp_ss_sheet.iloc[row_index, 'sector']

            # từ tổng GDP năm, tính lại gdp quý 4 từ các quý 1 2 3 trong năm

            # load lên silver với schema nào đó.

                




# TRÍCH XUẤT DỮ LIỆU THƯƠNG MẠI QUỐC TẾ
def extract_intenational_ecommerce_data_sheet_02(sheet : pd.DataFrame, type : str, month):
    # xóa các row không cần thiết
    num_of_remove_row = 0
    for i in range(len(sheet)):
        num_of_remove_row += 1
        if isinstance(sheet.iloc[i, 0], str) and 'mathangchuyeu' in clean_text(sheet.iloc[i, 0]): break

    if type == 'import':
        sheet = sheet.iloc[num_of_remove_row:len(sheet) - 1, ::].reset_index(drop =True)
        
    else: sheet = sheet.iloc[num_of_remove_row::, ::].reset_index(drop =True)

    name_colums = ['product_name', f'quantity_of_month_{month}', f'value_of_month_{month}']
    #xoa cac cot kh can thiet
    sheet = sheet.iloc[::, [1, 2, 3]]
    sheet.columns = name_colums

    if type == 'import': 
        for i in range(len(sheet)):
            if 'oto' == clean_text(sheet.loc[i, 'product_name']):
                sheet.loc[i, 'product_name'] = 'Ô tô và linh kiện'
            if 'Trong đó: Nguyên chiếc(*)' in sheet.loc[i, 'product_name'] :
                sheet.loc[ i, 'product_name'] = 'Ô tô nguyên chiếc' 
        
    return sheet

def extract_intenational_ecommerce_data_sheet_01(sheet : pd.DataFrame, type: str, month):
    # xóa các row không cần thiết
    num_of_remove_row = 0
    for i in range(len(sheet)):
        num_of_remove_row += 1
        if isinstance(sheet.iloc[i, 0], str) and 'mathangchuyeu' in clean_text(sheet.iloc[i, 0]): break

    if type == 'import':
        sheet = sheet.iloc[num_of_remove_row:len(sheet) - 1, ::].reset_index(drop =True)
        
    else: sheet = sheet.iloc[num_of_remove_row::, ::].reset_index(drop =True)

    name_colums = ['product_name', f'quantity_of_month_{month - 1}', f'value_of_month_{month -1}', f'quantity_of_month_{month}', f'value_of_month_{month}']
    #xoa cac cot kh can thiet
    sheet = sheet.iloc[::, [1, 2, 3, 5 ,6]]
    sheet.columns = name_colums
    if type == 'import': sheet.loc[ 29, 'product_name'] = 'Ô tô-nguyên chiếc' 
    return sheet

def extract_data_from_International_Ecommerce(excel_file: pd.ExcelFile, year, month):
    all_sheets = excel_file.sheet_names
    import_sheet = None
    export_sheet = None
    # code xác định sheet báo cáo dữ liệu thương mại quốc tế
    for i in range(len(all_sheets)):
        sheet_name = clean_text[all_sheets[i]]
        if any(name in sheet_name for name in ['nk', 'nhapkhau']) and all(name not in sheet_name for name in ['quy', 'gia']):
            import_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
        if any(name in sheet_name for name in ['xuatkhau', 'xk']) and all(name not in sheet_name for name in ['quy', 'gia']):
            export_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)

    if year > 2018 or year == 2018 and month >= 9 :
        # gọi hàm trích xuất được thiết kế ở trên 
        import_sheet = extract_intenational_ecommerce_data_sheet_02(import_sheet, 'import', month)
        export_sheet = extract_intenational_ecommerce_data_sheet_02(export_sheet, 'export', month)
        # load lên silver với 1 schema nào đó
        
    else:
        import_sheet = extract_intenational_ecommerce_data_sheet_01(import_sheet, 'import', month)
        export_sheet = extract_intenational_ecommerce_data_sheet_01(import_sheet, 'export', month)
        # từ sheet trích xuất dữ liệu và load lên silver theo 1 schema nào đó


def extract_data_from_Invesment(excel_file: pd.ExcelFile, year, month):
    next

def extract_data_from_Investment_by_Sector(excel_file: pd.ExcelFile, year, month):
    next

def extract_data_from_Labor_Market(excel_file: pd.ExcelFile, year, month):
    next

def extract_data_for_Product_Effection_fact(excel_file: pd.ExcelFile, year, month):
    next


def main_func():
    # lấy tất cả các đường dẫn trong bronze

    bucket_name = 'bronze'
    prefix = 'economic_report_excel_files/'

    objects = get_list_files(bucket_name, prefix)

    # duyệt qua từng đường dẫn đọc file và trích xuất dữ liệu
    for obj in objects:
        
        parts = str.split(obj, '/')

        year = parts[1]
        month = parts[2]


        excel_file = get_excel_file(bucket_name, obj)
        
        extract_data_from_GDP(excel_file, year, month)

        extract_data_from_International_Ecommerce(excel_file, year, month)

        extract_data_from_Invesment(excel_file, year, month)

        extract_data_from_Investment_by_Sector(excel_file, year, month)

        extract_data_from_Labor_Market(excel_file, year, month)

        extract_data_for_Product_Effection_fact(excel_file, year, month)


main_func()