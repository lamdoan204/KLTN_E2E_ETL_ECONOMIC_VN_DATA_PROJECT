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
                gdp_sheet['sector_and_sub_sector'] = (
                    gdp_sheet['sector_and_sub_sector']
                    .str.replace('\n', ' ', regex=False)
                    .str.replace(r'\s+', ' ', regex=True)
                    .str.strip()
                )
                gdp_sheet['sector_and_sub_sector'] = (
                    gdp_sheet['sector_and_sub_sector']
                    .replace({
                        'Nông lâm nghiệp và thuỷ sản': 'Nông, lâm nghiệp và thủy sản',
                        'Nông lâm nghiệp và thủy sản': 'Nông, lâm nghiệp và thủy sản'
                    })
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
