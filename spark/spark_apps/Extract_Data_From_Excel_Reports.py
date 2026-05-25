import pyspark.pandas as pd

from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *

# TRÍCH XUẤT DỮ LIỆU GPD VIỆT NAM THEO CÁC NGÀNH KINH TẾ
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
            column_names = ['sector_and_sub_sector', f'current_value', f'comparative_value']
            unit = gdp_sheet.iloc[5,2].replace('(', '').replace(')', '')
            gdp_sheet = gdp_sheet.iloc[::, [1,2,5]]
            gdp_sheet.columns = column_names
            # xóa các row thừa
            num_of_row_del = 0
            sector_column = gdp_sheet['sector_and_sub_sector']
            for row in sector_column:
                if isinstance(row, str): break
                num_of_row_del += 1
            gdp_sheet = gdp_sheet.iloc[num_of_row_del::, ::].reset_index(drop= True)

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
            current_df['year'], comparative_df['year'] = year
            current_df['quarter'], comparative_df['quarter'] = quarter
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
            

            insert_df_to_table_silver_layer(current_df, 'gdp')
            insert_df_to_table_silver_layer(comparative_df, 'gdp')

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
            unit = None
            for i in range(len(gdp_hh_sheet)):
                if isinstance(gdp_hh_sheet.iloc[i, 7], str): 
                    unit = gdp_hh_sheet.iloc[i, 7]
                    break
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

            insert_df_to_table_silver_layer(gdp_ss_sheet, 'gdp')
            insert_df_to_table_silver_layer(gdp_hh_sheet, 'gdp')
            
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
            unit = str(gdp_hh_sheet.iloc[3, 2]).split('(')[1].split(')')[0]
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

            insert_df_to_table_silver_layer(gdp_hh_sheet, 'gdp', year)
            insert_df_to_table_silver_layer(gdp_ss_sheet, 'gdp', year)




                
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
    # xoa cac cot kh can thiet
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

        # từ sheet trích xuất dữ liệu và load vào silver theo 1 schema nào đó



# TRÍCH XUẤT DỮ LIỆU ĐẦU TƯ KINH TẾ -  VỐN ĐẦU TƯ TOÀN XÃ HỘI
def extract_data_from_Invesment(excel_file: pd.ExcelFile, year, month):
    # kiểm tra phải file báo cáo theo ở quý không
    if month % 3 != 0 : return
    quarter = month / 3
    all_sheets = excel_file.sheet_names
    # xác định sheet chứa dữ liệu VDTTXH
    vdt_sheet = None
    for i in range(len(all_sheets)):
        current_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
        for index in range(len(current_sheet[0])): # duyệt qua cột 0 của sheet để láy title
            if isinstance(current_sheet.iloc[index, 0], str) and all(title in clean_text(current_sheet.iloc[index, 0]) for title in ['vondautu', 'thuchientoanxahoi','giahienhanh']):
                vdt_sheet = current_sheet
                break
    if(vdt_sheet is None):
        print(f"KHONG TIM THAY SHEET BAO CAO VDTTXH TRONG EXCEL FILE: year_{year}, month_{month} !!!!!!!!!")
        return
    # trích xuất dữ liệu
    # lấy các cột càn thiết
    vdt_sheet = vdt_sheet.iloc[::, 1:4]
    column_names = ['investmen_type', f'vale_of_quarter_{quarter - 1}', f'value_of_quarter_{quarter}']
    vdt_sheet.columns = column_names
    # xóa các hàng không cần thiết
    num_of_removed_col = -1
    for i in range(len(vdt_sheet['investment_type'])):
        num_of_removed_col += 1
        if isinstance(vdt_sheet.iloc[i, 'investment_type'], str):
            break
    vdt_sheet = vdt_sheet.iloc[num_of_removed_col::, ::].reset_index(drop= True)
    # load lên silver layer với 1 schema nào đó

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
    
    lamnghiep_title =  'ketquasanxuatlamnghiep'
    thuysan_title = 'sanluongthuysan'
    channuoi_title= 'sanphamchannuoi'
    chuyeu_title= 'sanluongmotsocaytrongchuyeu'
    hangnam_title= 'sanluongmotsocaytronghangnam'
    launam_title= 'sanluongmotsocaytronglaunam'

    title_list = [lamnghiep_title, thuysan_title, channuoi_title, chuyeu_title, hangnam_title, launam_title]

    col_0 = sheet.iloc[::, 0]
    i = 0
    for row in col_0:
        if isinstance(row, str) and title_sheet in clean_text(row):
            start_index = i
            continue
        if isinstance(row, str) and any(title in clean_text(row) for title in title_list) and start_index != -1:
            end_index = i + 1
            break
        i += 1
    
    return start_index, end_index

# sản phẩm công nghiệp
def extract_primarily_industry_product_data(sheet : pd.read_excel, month):

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
    return sheet

def extract_data_for_Product_Productivity_fact(excel_file: pd.ExcelFile, year, month):
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
        df = extract_primarily_industry_product_data(pd.read_excel(excel_file, sheet_name= all_sheets[san_pham_cong_nghiep_index], header= None), month= month)
        # load lên silver với schema nào đó
    else: 
        # trích xuất sản phẩm ngành công nghiệp
        # trích xuất dữ liệu còn lại của quý
        quarter = month / 3
        
        
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

        if quarter == 4:
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
        industry_product_df = extract_primarily_industry_product_data(pd.read_excel(excel_file, sheet_name= all_sheets[san_pham_cong_nghiep_index], header= None), month= month)
            
        # insert theo quý
            # chăn nuôi
        channuoi_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[chan_nuoi_sheet_index], header= None)
        start_index, end_index = search_start_and_end_index(channuoi_title, channuoi_sheet)
        channuoi_sheet = channuoi_sheet.iloc[start_index:end_index, [0, 2]].dropna().reset_index(drop= True)
        column_name = ['livestock_indicator', 'value'] #đặt tên cho cột
        channuoi_sheet.columns = column_name
        channuoi_sheet["unit"] = channuoi_sheet['livestock_indicator'].str.extract(r"\((.*?)\)") 
        channuoi_sheet['livestock_indicator'] = channuoi_sheet['livestock_indicator'].str.replace(r'\s*\(.*?\)', '', regex=True) .str.strip()     
        
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

            # thủy sản
        thuysan_sheet =  pd.read_excel(excel_file, sheet_name= all_sheets[thuy_san_sheet_index])
        start_index, end_index = search_start_and_end_index(thuysan_title, thuysan_sheet)
        # trích unit cho df
        unit = thuysan_sheet.iloc[start_index: end_index, ::].reset_index(drop= True).iloc[2, 6]
        thuysan_sheet = thuysan_sheet.iloc[start_index: end_index, [0, 2]].dropna().reset_index(drop= True)
        column_name = ['aquatic_type', 'value'] # đặt tên cho cột
        thuysan_sheet.columns = column_name
        thuysan_sheet['unit'] = unit
        idx = thuysan_sheet[thuysan_sheet['aquatic_type'].str.strip() == 'Nuôi trồng'].index[0]
        thuysan_sheet = thuysan_sheet.loc[idx:].reset_index(drop=True)

        # insert theo năm
        if quarter == 4:
            next
            # lâu năm
            caylaunam_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[cay_lau_nam_sheet_index], header= None)
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
            column_name = [ 'product' , 'value']
            area_df.columns = column_name
            production_df.columns = column_name

            production_df['unit'] = production_unit.group(1)
            area_df['unit'] = area_unit.group(1)        

            # hằng năm
            cayhangnam_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[cay_hang_nam_sheet_index], header= None)
            start_index, end_index = search_start_and_end_index(hangnam_title, cayhangnam_sheet)
            cayhangnam_sheet = cayhangnam_sheet.iloc[start_index + 1: end_index, [0, 2]].reset_index(drop= True)
            
            cayhangnam_sheet[2] = cayhangnam_sheet[2].fillna(' ')
            cayhangnam_sheet = cayhangnam_sheet.dropna().reset_index(drop= True)
            column_name  = ['product', 'values_year']
            cayhangnam_sheet.columns = column_name
            cayhangnam_sheet["unit"] = cayhangnam_sheet['product'].str.extract(r"\((.*?)\)").fillna(' ')
            
            # cây chủ yếu
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
            
    


def main_func():
    # lấy tất cả các đường dẫn trong bronze

    bucket_name = 'bronze'
    prefix = 'economic_report_excel_files/'

    objects = get_list_files(bucket_name, prefix)

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


        # extract_data_from_GDP(excel_file, year, month)

        # extract_data_from_International_Ecommerce(excel_file, year, month)

        # extract_data_from_Invesment(excel_file, year, month)

        # extract_data_from_Investment_by_Sector(excel_file, year, month)

        # # extract_data_from_Labor_Market(excel_file, year, month)

        # extract_data_for_Product_Productivity_fact(excel_file, year, month)


main_func()