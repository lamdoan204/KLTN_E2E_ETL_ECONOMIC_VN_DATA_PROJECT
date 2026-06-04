import pyspark.pandas as pd

from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *

# TRÍCH XUẤT DỮ LIỆU GPD VIỆT NAM THEO CÁC NGÀNH KINH TẾ
def extract_data_from_GDP(excel_file: pd.ExcelFile, year, month):
    try:
        # Kiểm tra phải báo cáo của quý không
        if month % 3 == 0:
            quarter = int( (month -1) / 3) + 1 
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
                column_names = ['sector_and_sub_sector', f'current_value', f'comparative_value']
                unit = 'Tỷ đồng'
                gdp_sheet_new = gdp_sheet.iloc[::, [0, 1, 2, 5]]

                gdp_sheet_new.iloc[:,1] = (
                    gdp_sheet_new.iloc[:,1]
                    .fillna(gdp_sheet.iloc[:,0])
                )
                gdp_sheet = gdp_sheet_new.iloc[::, 1: 4].dropna()
                gdp_sheet.columns = column_names
                # xóa các row thừa
                num_of_row_del = 0
                sector_column = gdp_sheet['sector_and_sub_sector']
                for row in sector_column:
                    if isinstance(row, str): break
                    num_of_row_del += 1
                gdp_sheet = gdp_sheet.iloc[num_of_row_del::, ::].reset_index(drop= True)
                gdp_sheet = gdp_sheet.iloc[1:,::]
                gdp_sheet['sector_and_sub_sector'] =(
                                                            gdp_sheet['sector_and_sub_sector']
                                                            .str.replace('\n', ' ', regex=False)
                                                            .str.replace(r'\s+', ' ', regex=True)
                                                            .str.strip()
                                                        )
                # code load lên silver theo schema nào đó
                current_df = gdp_sheet.iloc[::, [0, 1]]
                comparative_df = gdp_sheet.iloc[:: , [0, 2]]

                sectors = ['Nông, lâm nghiệp và thủy sản', 'Công nghiệp và xây dựng', 'Dịch vụ']

                current_df['unit'] = comparative_df['unit'] = unit
                current_df['type'], comparative_df['type'] = 'Giá trị hiện hành', 'Giá trị so sánh'
                current_df['year'] = comparative_df['year'] = year
                current_df['quarter'] = comparative_df['quarter'] = quarter
                current_df['sector'] = comparative_df['sector'] = current_df['sector_and_sub_sector'].where(
                                            current_df['sector_and_sub_sector'].isin(sectors)
                                            )
                current_df['sector'] = comparative_df['sector'] = current_df['sector'].ffill()

                current_df['sub_sector'] = comparative_df['sub_sector'] = current_df['sector_and_sub_sector']
                current_df = current_df[current_df['sector'] != current_df['sub_sector']]
                comparative_df = comparative_df[comparative_df['sector'] != comparative_df['sub_sector']]
                current_df['ingest_at'] = comparative_df['ingest_at'] = pd.Timestamp.now()
                current_df['sub_sector'] = current_df['sub_sector'].str.replace(';', ',').str.strip()
                comparative_df['sub_sector'] = comparative_df['sub_sector'].str.replace(';', ',').str.strip()
                current_df['value'] = current_df['current_value']
                comparative_df['value'] = comparative_df['comparative_value']

                comparative_df = comparative_df[comparative_df['sub_sector'] != 'Công nghiệp'].reset_index(drop= True)
                current_df = current_df[current_df['sub_sector'] != 'Công nghiệp'].reset_index(drop= True)
                
                current_df = current_df.iloc[::, 2:]
                comparative_df = comparative_df.iloc[::, 2:]

                insert_df_to_table_silver_layer(current_df, 'gdp', year, quarter)
                insert_df_to_table_silver_layer(comparative_df, 'gdp', year, quarter)

                return

            elif year >= 2019:
                
                gdp_hh_sheet = None
                gdp_ss_sheet = None
                sectors = ['Nông, lâm nghiệp và thủy sản', 'Công nghiệp và xây dựng', 'Dịch vụ']
                
                for i in range(len(all_sheets)):
                    current_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
                    title = clean_text(current_sheet.iloc[0, 0])


                    if 'tongsanphamtrongnuoctheogiahienhanh' in title and gdp_hh_sheet is None :
                        gdp_hh_sheet = current_sheet
                        continue
                    if 'tongsanphamtrongnuoctheogiasosanh' in title and gdp_ss_sheet is None: 
                        gdp_ss_sheet = current_sheet
                        break
                    
                    if gdp_ss_sheet is None or gdp_ss_sheet is None:
                        return
                        print(f"Không xác định được sheet báo cáo GDP trong file báo cáo năm: {year}, tháng: {month} =========")   

                # trích xuất dữ liệu
                    # lấy đơn vị 
                unit = 'Tỷ đồng'
                # lấy các cột cần thiết
                gdp_hh_sheet = gdp_hh_sheet.iloc[::, [1,3]]
                gdp_ss_sheet = gdp_ss_sheet.iloc[::,[1,3]]
                # đặt lại tên cho các cột
                column_names = ['sector_and_sub_sector', 'value']
                gdp_hh_sheet.columns = column_names
                gdp_ss_sheet.columns = column_names
                # xóa các hàng không cần thiết
                num_of_row_del = 0
                sector_column = gdp_hh_sheet['sector_and_sub_sector']
                for row in sector_column:
                    if isinstance(row, str): break
                    num_of_row_del += 1
                gdp_hh_sheet = gdp_hh_sheet.iloc[num_of_row_del::, ::].reset_index(drop= True)
                num_of_row_del = 0
                sector_column = gdp_ss_sheet['sector_and_sub_sector']
                for row in sector_column:
                    if isinstance(row, str): break
                    num_of_row_del += 1
                gdp_ss_sheet = gdp_ss_sheet.iloc[num_of_row_del::, ::].reset_index(drop= True)
                
                # code load lên silver theo schema nào đó
                gdp_hh_sheet['sector_and_sub_sector'] = (
                        gdp_hh_sheet['sector_and_sub_sector']
                        .str.replace('\n', ' ', regex=False)
                        .str.replace(r'\s+', ' ', regex=True)
                        .str.strip()
                    )

                gdp_ss_sheet['sector_and_sub_sector'] = (
                        gdp_ss_sheet['sector_and_sub_sector']
                        .str.replace('\n', ' ', regex=False)
                        .str.replace(r'\s+', ' ', regex=True)
                        .str.strip()
                    )
                gdp_ss_sheet['unit'] = gdp_hh_sheet['unit'] = unit
                gdp_hh_sheet['type'], gdp_ss_sheet['type'] = 'Giá trị hiện hành', 'Giá trị so sánh'
                gdp_ss_sheet['ingest_at'] = gdp_hh_sheet['ingest_at'] = pd.Timestamp.now()
                gdp_ss_sheet['year'] = gdp_hh_sheet['year'] = year
                gdp_ss_sheet['quarter'] = gdp_hh_sheet['quarter'] = quarter
                gdp_ss_sheet['sector'] = gdp_hh_sheet['sector'] = gdp_hh_sheet['sector_and_sub_sector'].where(
                                                        gdp_hh_sheet['sector_and_sub_sector'].isin(sectors)
                                                        )
                gdp_ss_sheet['sector'] = gdp_hh_sheet['sector'] = gdp_hh_sheet['sector'].ffill()
                gdp_ss_sheet['sub_sector'] = gdp_ss_sheet['sector_and_sub_sector']
                gdp_ss_sheet = gdp_ss_sheet[gdp_ss_sheet['sector'] != gdp_ss_sheet['sub_sector']]
                gdp_hh_sheet['sub_sector'] = gdp_hh_sheet['sector_and_sub_sector']
                gdp_hh_sheet = gdp_hh_sheet[gdp_hh_sheet['sector'] != gdp_hh_sheet['sub_sector']]

                gdp_hh_sheet['sub_sector'] = gdp_hh_sheet['sub_sector'].str.replace(';', ',').str.strip()
                gdp_ss_sheet['sub_sector'] = gdp_ss_sheet['sub_sector'].str.replace(';', ',').str.strip()

                gdp_ss_sheet = gdp_ss_sheet[gdp_ss_sheet['sub_sector'] != 'Công nghiệp'].reset_index(drop= True)
                gdp_hh_sheet = gdp_hh_sheet[gdp_hh_sheet['sub_sector'] != 'Công nghiệp'].reset_index(drop= True)

                insert_df_to_table_silver_layer(gdp_ss_sheet, 'gdp', year, quarter)
                insert_df_to_table_silver_layer(gdp_hh_sheet, 'gdp', year, quarter)
                
            else: # trích xuất dữ liệu trước 2018 quý 4
                gdp_hh_sheet = None
                gdp_ss_sheet = None
                
                for i in range(len(all_sheets)):
                    current_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
                    title = clean_text(current_sheet.iloc[0, 0])


                    if 'tongsanphamtrongnuoctheogiahienhanh' in title and gdp_hh_sheet is None :
                        gdp_hh_sheet = current_sheet
                        continue
                    if 'tongsanphamtrongnuoctheogiasosanh' in title and gdp_ss_sheet is None: 
                        gdp_ss_sheet = current_sheet
                        break
                    
                    if gdp_ss_sheet is None or gdp_ss_sheet is None:
                        print(f"Không xác định được sheet báo cáo GDP trong file báo cáo năm: {year}, tháng: {month} =========")   
                        return
                # Clean Data
                # lấy những cột cần thiết
                unit = 'Tỷ đồng'
                gdp_hh_sheet = gdp_hh_sheet.iloc[::, [0,1,3]]
                gdp_ss_sheet = gdp_ss_sheet.iloc[::, [0, 1, 3]]
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

                column_names = ['sector', 'sub_sector', 'value']
                gdp_hh_sheet.columns = column_names
                gdp_ss_sheet.columns = column_names


                # từ tổng GDP năm, tính lại gdp quý 4 từ các quý 1 2 3 trong năm

                # load lên silver với schema nào đó.
                gdp_ss_sheet['sector'] = (
                        gdp_ss_sheet['sector']
                        .str.replace('\n', ' ', regex=False)
                        .str.replace(r'\s+', ' ', regex=True)
                        .str.strip()
                        )
                gdp_ss_sheet['sub_sector'] = (
                                    gdp_ss_sheet['sub_sector']
                                    .str.replace('\n', ' ', regex=False)
                                    .str.replace(r'\s+', ' ', regex=True)
                                    .str.strip()
                                    )

                gdp_ss_sheet.iloc[len(gdp_ss_sheet)-1, 1] = gdp_ss_sheet.iloc[len(gdp_ss_sheet) - 1, 0]
                gdp_ss_sheet.iloc[len(gdp_ss_sheet) - 1, 0] = None
                gdp_ss_sheet['sector'] = gdp_ss_sheet['sector'].ffill()
                gdp_ss_sheet = gdp_ss_sheet.dropna().reset_index(drop= True)

                gdp_ss_sheet['sub_sector'] = gdp_ss_sheet['sub_sector'].str.replace(';', ',')


                gdp_hh_sheet['sector'] = (
                                    gdp_hh_sheet['sector']
                                    .str.replace('\n', ' ', regex=False)
                                    .str.replace(r'\s+', ' ', regex=True)
                                    .str.strip()
                                    )
                gdp_hh_sheet['sub_sector'] = (
                                    gdp_hh_sheet['sub_sector']
                                    .str.replace('\n', ' ', regex=False)
                                    .str.replace(r'\s+', ' ', regex=True)
                                    .str.strip()
                                    )

                gdp_hh_sheet.iloc[len(gdp_hh_sheet)-1, 1] = gdp_hh_sheet.iloc[len(gdp_hh_sheet) - 1, 0]
                gdp_hh_sheet.iloc[len(gdp_hh_sheet) - 1, 0] = None
                gdp_hh_sheet['sector'] = gdp_hh_sheet['sector'].ffill()
                gdp_hh_sheet = gdp_hh_sheet.dropna().reset_index(drop= True)

                gdp_hh_sheet['sub_sector'] = gdp_hh_sheet['sub_sector'].str.replace(';', ',')


                gdp_hh_sheet['quarter'] = gdp_ss_sheet['quarter'] = quarter
                gdp_hh_sheet['year'] = gdp_ss_sheet['year'] = year

                gdp_hh_sheet['type'], gdp_ss_sheet['type'] = 'Giá trị hiện hành', 'Giá trị so sánh'
                gdp_hh_sheet['ingest_at'] = gdp_ss_sheet['ingest_at'] = pd.Timestamp.now()
                gdp_hh_sheet['unit'] = gdp_ss_sheet['unit'] = unit

                gdp_ss_sheet = gdp_ss_sheet[gdp_ss_sheet['sub_sector'] != 'Công nghiệp'].reset_index(drop= True)
                gdp_hh_sheet = gdp_hh_sheet[gdp_hh_sheet['sub_sector'] != 'Công nghiệp'].reset_index(drop= True)

                insert_df_to_table_silver_layer(gdp_hh_sheet, 'gdp', year, quarter)
                insert_df_to_table_silver_layer(gdp_ss_sheet, 'gdp', year, quarter)
    except Exception as e:
        print(f'CÓ VẤN ĐỀ XẢY RA KHI TRÍCH XUẤT DỮ LIỆU GDP  {year}, THÁNG {month}', e)



                
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

    if year > 2018 or year == 2018 and month >= 9 :
        # gọi hàm trích xuất được thiết kế ở trên 
        import_sheet = extract_intenational_ecommerce_data_sheet_02(import_sheet, 'Import', month, year)
        export_sheet = extract_intenational_ecommerce_data_sheet_02(export_sheet, 'Export', month, year)
        # load lên silver với 1 schema nào đó
        insert_df_to_table_silver_layer(import_sheet, 'international_ecommerce', year, month)
        insert_df_to_table_silver_layer(export_sheet, 'international_ecommerce', year, month)
        
    else:
        import_sheet = extract_intenational_ecommerce_data_sheet_01(import_sheet, 'Import', month, year)
        export_sheet = extract_intenational_ecommerce_data_sheet_01(import_sheet, 'Export', month, year)
        # từ sheet trích xuất dữ liệu và load vào silver theo 1 schema nào đó
        insert_df_to_table_silver_layer(import_sheet, 'international_ecommerce', year, month)
        insert_df_to_table_silver_layer(export_sheet, 'international_ecommerce', year, month)


# TRÍCH XUẤT DỮ LIỆU ĐẦU TƯ KINH TẾ -  VỐN ĐẦU TƯ TOÀN XÃ HỘI
def extract_data_from_Invesment(excel_file: pd.ExcelFile, year, month):
    try:
        if month % 3 != 0 : return
        quarter = int((month - 1) / 3)  + 1 
        all_sheets = excel_file.sheet_names
        # xác định sheet chứa dữ liệu VDTTXH
        vdttxh_sheet_index = -1
        for i in range(len(all_sheets)):
            sheet_name = all_sheets[i]
            cleaned_sheet_name = clean_text(sheet_name)
            if 'ttxh' in cleaned_sheet_name:
                vdttxh_sheet_index = i
                break
        if(vdttxh_sheet_index is None):
            print(f"KHONG TIM THAY SHEET BAO CAO VDTTXH TRONG EXCEL FILE: year_{year}, month_{month} !!!!!!!!!")
            return
        # trích xuất dữ liệu
        # lấy các cột càn thiết
        vdt_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[vdttxh_sheet_index], header= None)
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
        insert_df_to_table_silver_layer(vdt_sheet, 'investment', year, quarter)
    except Exception as e:
        print(f'CÓ VẤN ĐỀ XẢY RA KHI TRÍCH XUẤT DỮ LIỆU VỐN ĐẦU TƯ TOÀN XÃ HỘI NĂM {year}, THÁNG {month}', e)
    # kiểm tra quý nào thiếu thì trích từ file báo cáo excel của quý sau



# Cào dữ liệu về và trích xuất từ 1 file excel - TRÍCH XUẤT DỮ LIỆU VỐN ĐẦU TƯ CHẢY VÀO NGÀNH KINH TẾ NÀO
def extract_data_from_Investment_by_Sector(excel_file: pd.ExcelFile, year, month):
    next



# TRÍCH XUẤT DỮ LIỆU LIÊN QUAN ĐẾN THỊ TRƯỜNG LAO ĐỘNG - TỶ LỆ THẤT NGHIỆP, ĐỘ TUỔI THẤT NGHIỆP - NÔNG THÔN THÀNH THỊ
# import time
# def extract_data_from_Labor_Market(excel_file: pd.ExcelFile, year, month):
#     all_sheets = excel_file.sheet_names
    
#     if month % 3 != 0 : return
#     quarter = month / 3
#     current_year = pd.Timestamp.now().year
    
#     # xác định sheet_index


#     if year < current_year:
#         if quarter != 4: return
#         else:
            
#             # trích xuất dữ liệu
#             next
#         # trích xuất dữ liệu dựa vào quý 4 của hằng năm
#     else:
#         # lấy tháng hiện tại, nó thuộc quý nào trong năm, nếu lớn hơn 1 thì trích từ file quý đó -1
        # next






# TRÍCH XUẤT DỮ LIỆU NĂNG SUẤT SẢN PHẨM - CÂY TRỒNG, VẬT NUÔI, LÂM NGHIỆP.
def search_start_and_end_index(title_sheet, sheet):
    start_index = -1
    end_index = len(sheet)
    congnghiep_title= 'motsosanphamnchuyeunganhcongnghiep'
    lamnghiep_title =  'ketquasanxuatlamnghiep'
    thuysan_title = 'sanluongthuysan'
    channuoi_title= 'sanphamchannuoi'
    chuyeu_title= 'sanluongmotsocaytrongchuyeu'
    hangnam_title= 'sanluongmotsocaycongnghiephangnam'
    launam_title= 'sanluongmotsocaycongnghiepnam'

    title_list = [lamnghiep_title, thuysan_title, channuoi_title, chuyeu_title, hangnam_title, launam_title]

    col_0 = sheet.iloc[::, 0]
    i = 0
    for row in col_0:
        if isinstance(row, str) and title_sheet in clean_text(row):
            start_index = i
            continue
        if isinstance(row, str) and any(title in clean_text(row) for title in title_list if title is not title_sheet ) and start_index != -1:
            end_index = i + 1
            break
        i += 1
    
    return start_index, end_index    

# sản phẩm công nghiệp
def extract_primarily_industry_product_data(sheet : pd.read_excel, month : int, year: int):

    column_names = ['product_name', 'unit', 'value']
    sheet = sheet.iloc[::, [0, 1, 3]]
    sheet.columns =column_names
    
    sheet = sheet.dropna().reset_index(drop= True)

    for row_index in range(len(sheet)):
        unit = sheet.loc[row_index, 'unit']
        if '\n' in unit:
            unit = unit.replace('\n', '')
            sheet.loc[row_index, 'unit'] = unit
        if unit == '\"':
            pre_unit = sheet.loc[row_index -1 , 'unit']
            sheet.loc[row_index, 'unit'] = pre_unit
    sheet['product_name'] = sheet['product_name'].str.replace('\n',' ').str.strip()
    sheet['month'] = month
    sheet['quarter'] = int(int((month -1)/3)  + 1)
    sheet['year'] = year
    sheet['ingest_at'] = pd.Timestamp.now()
    return sheet

def extract_data_for_Product_Productivity_fact(excel_file: pd.ExcelFile, year : int, month : int):
        try: 
            ##########################################################
            congnghiep_title= 'motsosanphamnchuyeunganhcongnghiep'
            lamnghiep_title =  'ketquasanxuatlamnghiep'
            thuysan_title = 'sanluongthuysan'
            channuoi_title= 'sanphamchannuoi'
            chuyeu_title= 'sanluongmotsocaytrongchuyeu'
            hangnam_title= 'sanluongmotsocaycongnghiephangnam'
            launam_title= 'sanluongmotsocaycongnghieplaunam'
            ##########################################################
            all_sheets  = excel_file.sheet_names
            san_pham_cong_nghiep_index = -1
            quarter = int((month - 1)/ 3) + 1
            congnghiep_title = 'motsosanphamchuyeucuanganhcongnghiep'
            if month % 3 != 0:
                # chỉ trích xuất dữ liệu theo tháng của sản phẩm ngành công nghiệp
                for i in range(len(all_sheets)):
                    current_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
                    col_0 = current_sheet.iloc[::, 0]
                    for row in range(len(col_0)):
                            if isinstance(col_0[row], str):
                                cleaned_text =clean_text(col_0[row])
                                if congnghiep_title == cleaned_text and san_pham_cong_nghiep_index == -1: san_pham_cong_nghiep_index = i
                    if san_pham_cong_nghiep_index != -1: break
                df = extract_primarily_industry_product_data(pd.read_excel(excel_file, sheet_name= all_sheets[san_pham_cong_nghiep_index], header= None), month= month, year =year)
                # load lên silver với schema nào đó
                insert_df_to_table_silver_layer(df, 'industry_product', year, quarter)
                return
            else: 
                # trích xuất sản phẩm ngành công nghiệp
                # trích xuất dữ liệu còn lại của quý
                quarter = int((month - 1) / 3) + 1
                
                lam_nghiep_sheet_index = -1
                thuy_san_sheet_index = -1
                chan_nuoi_sheet_index = -1

                cay_hang_nam_sheet_index = -1
                cay_lau_nam_sheet_index =-1
                cay_trong_chu_yeu_sheet_index = -1
                
                # xác định sheet index
                for i in range(len(all_sheets)):
                    current_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
                    col_0 = current_sheet.iloc[::, 0]
                    for row in range(len(col_0)):
                            if isinstance(col_0[row], str):
                                cleaned_text =clean_text(col_0[row])
                                if 'sanluongthuysan' in  cleaned_text and thuy_san_sheet_index == -1: thuy_san_sheet_index = i
                                if 'sanxuatlamnghiep' in cleaned_text and lam_nghiep_sheet_index == -1: lam_nghiep_sheet_index = i
                                if 'channuoi' in cleaned_text and chan_nuoi_sheet_index == -1: chan_nuoi_sheet_index = i
                                if congnghiep_title == cleaned_text and san_pham_cong_nghiep_index == -1: san_pham_cong_nghiep_index = i
                    print(san_pham_cong_nghiep_index)
                    if all(index != -1 for index in [lam_nghiep_sheet_index, thuy_san_sheet_index, chan_nuoi_sheet_index, san_pham_cong_nghiep_index]): 
                            break 

                if month == 12:
                    for i in range(len(all_sheets)):
                        current_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
                        col_0 = current_sheet.iloc[::, 0]

                        for row in range(len(col_0)):
                            if isinstance(col_0[row], str):
                                cleaned_text = clean_text(col_0[row])
                                if any(title in cleaned_text for title in ['cayhangnam', 'motsocaycongnghiephangnam']) and cay_hang_nam_sheet_index == -1:
                                    cay_hang_nam_sheet_index = i
                                if any(title in cleaned_text for title in ['caylaunam', 'caycongnghieplaunam']) and cay_lau_nam_sheet_index == -1:
                                    cay_lau_nam_sheet_index = i
                                if 'caytrongchuyeu' in cleaned_text and cay_trong_chu_yeu_sheet_index == -1:
                                    cay_trong_chu_yeu_sheet_index = i
                        if all(sheet_index != -1 for sheet_index in [cay_hang_nam_sheet_index, cay_trong_chu_yeu_sheet_index, cay_lau_nam_sheet_index]):
                            break
                # Bước trích xuất dữ liệu    
                # xử lý các sheet cùng sheet index - tách từng df ra
                
                # insert theo tháng
                    # sản phẩm công nghiệp
                industry_product_df = extract_primarily_industry_product_data(pd.read_excel(excel_file, sheet_name= all_sheets[san_pham_cong_nghiep_index], header= None), month= month, year= year)
                insert_df_to_table_silver_layer(industry_product_df, 'industry_product', year, quarter)

                # insert theo quý
                    # chăn nuôi
                channuoi_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[chan_nuoi_sheet_index], header= None)
                # start_index, end_index = search_start_and_end_index(channuoi_title, channuoi_sheet)
                channuoi_sheet = channuoi_sheet.iloc[::, [0, 2]]
                column_name = ['livestock_indicator', 'value'] #đặt tên cho cột
                channuoi_sheet.columns = column_name
                channuoi_sheet["unit"] = channuoi_sheet['livestock_indicator'].str.extract(r"\((.*?)\)").ffill()
                channuoi_sheet['livestock_indicator'] = channuoi_sheet['livestock_indicator'].str.replace(r'\s*\(.*?\)', '', regex=True) .str.strip() 
                channuoi_sheet = channuoi_sheet.dropna().reset_index(drop= True)
                channuoi_sheet['quarter'] = quarter
                channuoi_sheet['year'] = year
                channuoi_sheet['ingest_at'] = pd.Timestamp.now()
                insert_df_to_table_silver_layer(channuoi_sheet, 'livestock', year, quarter)

                # lâm nghiệp
                lamnghiep_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[lam_nghiep_sheet_index], header= None)
                start_index, end_index = search_start_and_end_index(lamnghiep_title, lamnghiep_sheet )
                lamnghiep_sheet = lamnghiep_sheet.iloc[start_index: end_index, [0, 2]].dropna().reset_index(drop= True)
                # trích xuất đơn vị cho df lamnghiep

                column_name = ['forestry_indicator', 'value'] #đặt tên cho cột
                lamnghiep_sheet.columns = column_name
                lamnghiep_sheet["unit"] = lamnghiep_sheet['forestry_indicator'].str.extract(r"\((.*?)\)")
                lamnghiep_sheet = lamnghiep_sheet.fillna('Ha')
                lamnghiep_sheet['forestry_indicator'] = lamnghiep_sheet['forestry_indicator'].str.replace(r'\s*\(.*?\)', '', regex=True) .str.strip() 
                lamnghiep_sheet['quarter'] = quarter
                lamnghiep_sheet['year'] = year
                lamnghiep_sheet['ingest_at'] = pd.Timestamp.now()
                insert_df_to_table_silver_layer(lamnghiep_sheet, 'forestry', year, quarter)

                    # thủy sản
                import numpy as np
                thuysan_sheet =  pd.read_excel(excel_file, sheet_name= all_sheets[thuy_san_sheet_index], header= None)
                if thuysan_sheet.shape[1] < 5:
                    start_index, end_index = search_start_and_end_index(thuysan_title, thuysan_sheet)
                    # trích unit cho df

                    unit = 'Nghìn tấn'

                    thuysan_sheet = thuysan_sheet.iloc[start_index: end_index, [0, 2]].dropna().reset_index(drop= True)
                    column_name = ['aquatic_type', 'value'] # đặt tên cho cột
                    thuysan_sheet.columns = column_name
                    thuysan_sheet['unit'] = unit

                    idx = thuysan_sheet[thuysan_sheet['aquatic_type'].str.strip() == 'Nuôi trồng'].index[0]
                    
                    thuysan_sheet = thuysan_sheet.loc[idx:].reset_index(drop=True)
                    # đánh dấu header group
                    thuysan_sheet['aquatic_group'] = np.where(
                        thuysan_sheet['aquatic_type'].isin(['Nuôi trồng','Khai thác']),
                        thuysan_sheet['aquatic_type'],
                        np.nan
                    )

                    # forward fill group
                    thuysan_sheet['aquatic_group'] = thuysan_sheet['aquatic_group'].ffill()

                    # loại bỏ dòng tổng
                    result = thuysan_sheet[
                        ~thuysan_sheet['aquatic_type'].isin(['Nuôi trồng','Khai thác'])
                    ].copy()

                    # đổi tên cột
                    result['product_name'] = result['aquatic_type']
                    result['aquatic_type'] = result['aquatic_group']

                    # chọn cột cuối
                    thuysan_sheet = result[
                        ['aquatic_type','product_name','value','unit']
                    ]
                else:
                    start_index, end_index = search_start_and_end_index(thuysan_title, thuysan_sheet)

                    unit = 'Nghìn tấn'

                    df = thuysan_sheet.iloc[start_index:end_index].copy()
                    print(df.shape)
                    print(df.columns)
                    print(df.head())

                    # chỉ giữ các cột cần dùng
                    df = df[[1,2,4]].copy()

                    # đổi tên cột
                    df.columns = ['aquatic_group','product_name','value']

                    # fill group (Nuôi trồng / Khai thác)
                    df['aquatic_group'] = df['aquatic_group'].ffill()

                    # bỏ các dòng rỗng
                    df = df.dropna(subset=['product_name'])

                    # chỉ lấy từ Nuôi trồng trở xuống
                    idx = df[df['aquatic_group'].str.strip() == 'Nuôi trồng'].index[0]
                    df = df.loc[idx:].reset_index(drop=True)

                    # loại bỏ dòng header group
                    result = df[
                        ~df['product_name'].isin(['Nuôi trồng','Khai thác'])
                    ].copy()

                    # thêm unit
                    result['unit'] = unit

                    # chọn cột cuối
                    thuysan_sheet = result[
                        ['aquatic_group','product_name','value','unit']
                    ].rename(columns={
                        'aquatic_group':'aquatic_type'
                    })

                thuysan_sheet['quarter'] = quarter
                thuysan_sheet['year'] = year
                thuysan_sheet['ingest_at']  = pd.Timestamp.now()
                insert_df_to_table_silver_layer(thuysan_sheet, 'aquatic_products', year, quarter)


                # insert theo năm
            if month == 12:
                    if cay_lau_nam_sheet_index != -1:
                        caylaunam_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[cay_lau_nam_sheet_index], header= None)
                        if year < 2025:
                        # lâu năm
                            start_index, end_index = search_start_and_end_index(launam_title, caylaunam_sheet)
                            caylaunam_sheet = caylaunam_sheet.iloc[start_index : end_index, [0,2]].reset_index(drop= True)
                                # tách df sản lượng và diện tích
                            start_index_area = -1 
                            start_index_production = -1 
                            i = 0
                            col_0 = caylaunam_sheet[0]
                            for row in col_0:
                                if isinstance(row, str) and 'dientichgieotrong' in clean_text(row): start_index_area = i    
                                if isinstance(row, str) and 'sanluongnghintan' in clean_text(row): start_index_production = i
                                i += 1
                            if start_index_area < start_index_production:
                                area_df = caylaunam_sheet.iloc[start_index_area: start_index_production, ::].reset_index(drop= True)
                                production_df = caylaunam_sheet.iloc[start_index_production: len(caylaunam_sheet), ::].reset_index(drop = True)
                            
                            area_unit = re.search(r"\((.*?)\)" ,area_df.iloc[0, 0])
                            production_unit = re.search(r"\((.*?)\)" , production_df.iloc[0, 0])
                            column_name = [ 'crop_name' , 'value']
                            area_df.columns = column_name
                            production_df.columns = column_name

                            production_df['unit'] = production_unit.group(1)
                            production_df = production_df.iloc[1:]
                            area_df['unit'] = area_unit.group(1)   
                            area_df = area_df.dropna()# đổi tên cột để tránh trùng
                            production_df = production_df.rename(columns={
                                'value': 'production',
                                'unit': 'production_unit'
                            })

                            area_df = area_df.rename(columns={
                                'value': 'area',
                                'unit' : 'area_unit'
                            })

                            # chuẩn hóa tên product để merge
                            production_df['crop_name'] = production_df['crop_name'].str.replace(r'\s*\(.*?\)', '', regex=True)

                            # merge
                            merged_df = production_df.merge(
                                area_df[['crop_name', 'area', 'area_unit']],
                                on='crop_name',
                                how='inner'
                            )

                            # tính năng suất
                            merged_df['yield'] = merged_df['production'] / merged_df['area'] * 10
                            merged_df['yield_unit'] = 'Tạ/ha'
                            merged_df['year'] =year
                            merged_df['ingest_at'] = pd.Timestamp.now()

                            insert_df_to_table_silver_layer(merged_df, 'perennial_crops', year, quarter)
                    
                        else:
                            caylaunam_sheet = caylaunam_sheet.iloc[::, [0, 2]]
                            unit = 'Nghìn tấn'
                            column_name = ['crop_name', 'production']
                            caylaunam_sheet.columns = column_name
                            caylaunam_sheet['production_unit'] = unit
                            caylaunam_sheet = caylaunam_sheet.dropna().reset_index(drop= True)
                            caylaunam_sheet['year'] = 'year'
                            caylaunam_sheet['yield'], caylaunam_sheet['area'], caylaunam_sheet['yield_unit'], caylaunam_sheet['area_unit'] = \
                            None, None, None, None
                            caylaunam_sheet['ingest_at'] = pd.Timestamp.now()
                            insert_df_to_table_silver_layer(caylaunam_sheet, 'perennial_crops', year, quarter)
                    else: print('Không công cố dữ liệu về cây lâu năm !!!!!!!!!!!!!!')
                        

                    # hằng năm
                    if cay_hang_nam_sheet_index != -1 :
                        cayhangnam_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[cay_hang_nam_sheet_index], header= None)
                        start_index, end_index = search_start_and_end_index(hangnam_title, cayhangnam_sheet)
                        cayhangnam_sheet = cayhangnam_sheet.iloc[start_index + 1: end_index, [0, 2]].reset_index(drop= True)
                        
                        cayhangnam_sheet[2] = cayhangnam_sheet[2].fillna(' ')
                        cayhangnam_sheet = cayhangnam_sheet.dropna().reset_index(drop= True)
                        column_name  = ['product', 'value']
                        cayhangnam_sheet.columns = column_name
                        cayhangnam_sheet["unit"] = cayhangnam_sheet['product'].str.extract(r"\((.*?)\)").fillna(' ')
                        import numpy as np

                        metrics_map = {
                            'Diện tích': 'area',
                            'Năng suất': 'yield',
                            'Sản lượng': 'production'
                        }

                        # xác định dòng crop
                        cayhangnam_sheet['crop_group'] = np.where(
                            ~cayhangnam_sheet['crop_name'].isin(metrics_map.keys()),
                            cayhangnam_sheet['crop_name'],
                            np.nan
                        )

                        cayhangnam_sheet['crop_group'] = cayhangnam_sheet['crop_group'].ffill()

                        # chỉ giữ metric rows
                        detail = cayhangnam_sheet[cayhangnam_sheet['crop_name'].isin(metrics_map.keys())].copy()

                        # đổi metric sang English
                        detail['metric'] = detail['crop_name'].map(metrics_map)

                        # pivot values
                        values_pivot = detail.pivot(
                            index='crop_group',
                            columns='metric',
                            values='values'
                        )

                        # pivot units
                        unit_pivot = detail.pivot(
                            index='crop_group',
                            columns='metric',
                            values='unit'
                        )

                        # build result
                        result = pd.DataFrame({
                            'crop_name': values_pivot.index,

                            'area': values_pivot['area'],
                            'area_unit': unit_pivot['area'],

                            'yield': values_pivot['yield'],
                            'yield_unit': unit_pivot['yield'],

                            'production': values_pivot['production'],
                            'production_unit': unit_pivot['production']
                        }).reset_index(drop=True)
                        cayhangnam_sheet = result
                        cayhangnam_sheet['year'] = year
                        cayhangnam_sheet['ingest_at'] = pd.Timestamp.now()
                        insert_df_to_table_silver_layer(cayhangnam_sheet,'annual_crops', year, quarter)
                    else: print("Không công bố dữ liệu về cây trồng hằng năm")
                    
                    
                    # cây chủ yếu
                    if cay_trong_chu_yeu_sheet_index != -1:
                        caychuyeu_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[cay_trong_chu_yeu_sheet_index], header= None)
                        start_index, end_index = search_start_and_end_index(chuyeu_title, caychuyeu_sheet)
                        caychuyeu_df = caychuyeu_sheet.iloc[start_index + 1 : end_index, [0, 1, 3]]    .reset_index(drop= True)


                        col_0 = caychuyeu_df[1]
                            # cây lương thực có hạt
                        start_index_1 = -1
                        end_index_1 = -1
                            # cây chất bột có củ
                        start_index_2 = -1

                        col_0 = caychuyeu_df[0]
                        i = 0
                        for row in col_0:
                            if isinstance(row, str) and 'cohat' in clean_text(row): start_index_1 = i
                            if isinstance(caychuyeu_df.iloc[i, 1], str) and 'tongsanluong' in clean_text(caychuyeu_df.iloc[i, 1]) : end_index_1 = i - 1
                            if isinstance(row, str) and 'cocu' in clean_text(row) and start_index_1 != -1 : start_index_2 = i
                            i += 1

                        column_name = ['product_and_infor', f'value_{year}']
                        cohat_df = caychuyeu_df.iloc[start_index_1:end_index_1, 1:].dropna(subset=[1]).reset_index(drop= True)
                        cocu_df = caychuyeu_df.iloc[start_index_2: len(caychuyeu_df), 1:].dropna(subset=[1]).reset_index(drop= True)
                        cohat_df.columns = column_name
                        cocu_df.columns = column_name
                        cohat_df['unit'] = cohat_df['product_and_infor'].str.extract(r"\((.*?)\)").fillna(' ')
                        cocu_df['unit'] = cohat_df['product_and_infor'].str.extract(r"\((.*?)\)").fillna(' ')

                        # load lên silver layer
                        def parse_crop_table(df, type_label):
                            # Xác định các hàng tên cây (value_2019 = NaN)
                            df = df.copy()
                            df['crop_name'] = df['product_and_infor'].where(df['value_2019'].isna()).ffill()
                        
                            # Bỏ các hàng tên cây, chỉ giữ hàng số liệu
                            df = df[df['value_2019'].notna()].copy()
                        
                            # Phân loại từng chỉ tiêu
                            df['metric'] = np.select(
                                [
                                    df['product_and_infor'].str.contains('Diện tích'),
                                    df['product_and_infor'].str.contains('Năng suất'),
                                    df['product_and_infor'].str.contains('Sản lượng'),
                                ],
                                ['area', 'yield', 'production'],
                                default='other'
                            )
                        
                            # Pivot: mỗi cây thành 1 hàng
                            values = df.pivot_table(index='crop_name', columns='metric', values='value_2019', aggfunc='first')
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
                        
                            # Sắp xếp cột
                            return result[['crop_name', 'type', 'area', 'area_unit', 'yield', 'yield_unit', 'production', 'production_unit']]
                        
                        
                        # ---- Gộp 2 bảng ----
                        merged_df = pd.concat(
                            [parse_crop_table(cocu_df, 'cây có củ'),
                            parse_crop_table(cohat_df, 'cây có hạt')],
                            ignore_index=True
                        )
                        merged_df['year'], merged_df['ingest_at'] = year, pd.Timestamp.now()
                        insert_df_to_table_silver_layer(merged_df, 'staple_crops', year, quarter, year, quarter)
            
                    else: print("Không công bố dữ liệu về cây trồng chủ yếu")

            else: print('Không phải dữ liệu quý 4 !!!!!!')
        except Exception as e:
            print(f'CÓ VẤN ĐỀ TRONG TRÍCH XUẤT DỮ LIỆU NĂNG SUẤT SẢN PHẨM năm : {year}, tháng {month}', e)
 
                        
         
def main_func():
    # lấy tất cả các đường dẫn trong bronze
    bucket_name = 'bronze'
    prefix = 'economic_report_excel_files/'

    objects = get_list_files(bucket_name, prefix)

    if objects is None:
        print("Không tìm thấy bất kỳ file báo cáo nào !!!!!!")
        return

    # duyệt qua từng đường dẫn đọc file và trích xuất dữ liệu
    for obj in objects:
        
        parts = str.split(obj, '/')

        year = int(parts[1])
        month = int(parts[2])


        excel_file = get_excel_file(bucket_name, obj)

        if excel_file is None: print('Đọc file Excel không thành công')
        
        print('duyệt qua từng đường dẫn đọc file và trích xuất dữ liệu')

        print(f'FILE EXCEL: YEAR : {year}, MONTH = {month} ')

        extract_data_from_GDP(excel_file, year, month)

        extract_data_from_International_Ecommerce(excel_file, year, month)

        extract_data_from_Invesment(excel_file, year, month)

        # extract_data_from_Investment_by_Sector(excel_file, year, month)

        extract_data_for_Product_Productivity_fact(excel_file, year, month)
        
    print(f"Tải thành công dữ liệu từ file: tháng: {month} - năm: {year} lên SILVER LAYER")

main_func()