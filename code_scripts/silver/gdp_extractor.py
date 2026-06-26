import pyspark.pandas as pd
import numpy as np
from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *

# HÀM BỔ TRỢ: Chuẩn hóa sub_sector theo yêu cầu của hệ thống
def clean_sub_sector(df: pd.DataFrame) -> pd.DataFrame:
    if 'sub_sector' not in df.columns:
        return df
        
    # 1. Thay thế dấu chấm phẩy thành dấu phẩy và loại bỏ khoảng trắng thừa
    df['sub_sector'] = df['sub_sector'].str.replace(';', ',', regex=False).str.strip()
    
    # 2. Xử lý khoảng trắng lặp và ký tự đặc biệt (đảm bảo đồng bộ trước khi replace map)
    df['sub_sector'] = df['sub_sector'].str.replace(r'\s+', ' ', regex=True).str.strip()

    # 3. Định nghĩa từ điển map chuẩn hóa (Mapping Dictionary)
    mapping_dict = {
        'Công nghiệp chế biến': 'Công nghiệp chế biến và chế tạo',
        'Công nghiệp chế biến, chế tạo': 'Công nghiệp chế biến và chế tạo',
        'HĐ làm thuê công việc GĐ trong các hộ tư nhân': 'Hoạt động làm thuê công việc gia đình trong các hộ tư nhân',
        'HĐ phục vụ cá nhân và cộng đồng': 'Hoạt động phục vụ cá nhân và cộng đồng',
        'Thuỷ sản': 'Thủy sản',
        'Vận tải, kho bãi': 'Vận tải kho bãi',
        'Vận tải, kho bãi và thông tin liên lạc': 'Vận tải kho bãi',
        'Y tế và hoạt động cứu trợ xã hội': 'Y tế và hoạt động trợ giúp xã hội'
        
    }
    
    # Áp dụng từ điển thay thế
    df['sub_sector'] = df['sub_sector'].replace(mapping_dict)
    
    # 4. Xử lý chuỗi dài phức tạp và chuẩn hóa khoảng trắng quanh dấu gạch ngang (-) cho ngành Đảng Cộng Sản / Nhà Nước
    # Đồng thời sửa các lỗi chuỗi bị dính/lặp văn bản nếu có
    def fix_long_sector(text):
        if not isinstance(text, str):
            return text
        # Chuẩn hóa khoảng trắng xung quanh dấu gạch ngang: "chính trị- xã hội" hoặc "chính trị-xã hội" -> "chính trị - xã hội"
        text = re.sub(r'chính trị\s*-\s*xã hội', 'chính trị - xã hội', text)
        
        # Xử lý trường hợp chuỗi dài bị lặp văn bản dính liền nhau (nếu có trong dữ liệu gốc)
        target_phrase = "Hoạt động của Đảng Cộng sản, tổ chức chính trị - xã hội, quản lý Nhà nước, an ninh quốc phòng, đảm bảo xã hội bắt buộc"
        if "Hoạt động của Đảng Cộng sản" in text:
            return target_phrase
        return text

    # Vì pyspark.pandas có thể hạn chế khi apply hàm tự định nghĩa phức tạp, ta dùng thủ thuật replace regex hoặc áp dụng an toàn
    df['sub_sector'] = df['sub_sector'].str.replace(r'chính trị\s*-\s*xã hội', 'chính trị - xã hội', regex=True)
    df['sub_sector'] = df['sub_sector'].str.replace(r'chính trị-\s*xã hội', 'chính trị - xã hội', regex=True)
    
    # Đảm bảo trường hợp text dài bị nhân đôi/lặp được đưa về cấu trúc chuẩn duy nhất
    # (Tìm kiếm đoạn text chứa từ khóa chính và ghi đè hoàn toàn bằng text chuẩn)
    df['sub_sector'] = np.where(
        df['sub_sector'].str.contains('Đảng Cộng sản', na=False),
        'Hoạt động của Đảng Cộng sản, tổ chức chính trị - xã hội, quản lý Nhà nước, an ninh quốc phòng, đảm bảo xã hội bắt buộc',
        df['sub_sector']
    )

    return df


# TRÍCH XUẤT DỮ LIỆU GPD VIỆT NAM THEO CÁC NGÀNH KINH TẾ
def extract_data_from_GDP(excel_file: pd.ExcelFile, year, month):
    try:
        # Kiểm tra phải báo cáo của quý không
        if month % 3 == 0:
            quarter = int((month - 1) / 3) + 1 
            all_sheets = excel_file.sheet_names

            # -----------------------------------------------------------------
            # TRƯỜNG HỢP 1: Quý 1 HOẶC (Trước/Bằng năm 2018 và trước/bằng Quý 3)
            # -----------------------------------------------------------------
            if quarter == 1 or (year <= 2018 and quarter <= 3): 
                gdp_sheet = None
                for i in range(len(all_sheets)):
                    if 'gdp' in str.lower(all_sheets[i]):
                        gdp_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[i], header=None)

                if gdp_sheet is None:
                    print(f"Không xác định được sheet báo cáo GDP trong file báo cáo năm: {year}, tháng: {month} ===========")      
                    return
                
                column_names = ['sector_and_sub_sector', 'current_value', 'comparative_value']
                unit = 'Tỷ đồng'
                gdp_sheet_new = gdp_sheet.iloc[:, [0, 1, 2, 5]]

                gdp_sheet_new.iloc[:, 1] = gdp_sheet_new.iloc[:, 1].fillna(gdp_sheet.iloc[:, 0])
                gdp_sheet = gdp_sheet_new.iloc[:, 1:4].dropna()
                gdp_sheet.columns = column_names
                
                num_of_row_del = 0
                sector_column = gdp_sheet['sector_and_sub_sector']
                for row in sector_column:
                    if isinstance(row, str): break
                    num_of_row_del += 1
                gdp_sheet = gdp_sheet.iloc[num_of_row_del:, :].reset_index(drop=True)
                gdp_sheet = gdp_sheet.iloc[1:, :]
                
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
                
                current_df = gdp_sheet.iloc[:, [0, 1]].copy()
                comparative_df = gdp_sheet.iloc[:, [0, 2]].copy()

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
                
                current_df['value'] = current_df['current_value']
                comparative_df['value'] = comparative_df['comparative_value']

                comparative_df = comparative_df[comparative_df['sub_sector'] != 'Công nghiệp']
                current_df = current_df[current_df['sub_sector'] != 'Công nghiệp']
                
                current_df = current_df.iloc[:, 2:]
                comparative_df = comparative_df.iloc[:, 2:]

                # CHUẨN HÓA DỮ LIỆU SUB_SECTOR TRƯỚC KHI INSERT SILVER
                current_df = clean_sub_sector(current_df).reset_index(drop=True)
                comparative_df = clean_sub_sector(comparative_df).reset_index(drop=True)

                current_df['value'] = pd.to_numeric(current_df['value'], errors= 'coerce').round(3)
                comparative_df['value'] = pd.to_numeric(comparative_df['value'], errors= 'coerce').round(3)
                current_df = current_df.drop_duplicates()
                current_df = current_df.dropna()
                
                insert_df_to_table_silver_layer(current_df, 'gdp', year, quarter)
                insert_df_to_table_silver_layer(comparative_df, 'gdp', year, quarter)
                return

            # -----------------------------------------------------------------
            # TRƯỜNG HỢP 2: Từ năm 2019 trở đi (Trừ Q1 đã xử lý ở trên)
            # -----------------------------------------------------------------
            elif year >= 2019:
                gdp_hh_sheet = None
                gdp_ss_sheet = None
                sectors = ['Nông, lâm nghiệp và thủy sản', 'Công nghiệp và xây dựng', 'Dịch vụ']
                
                for i in range(len(all_sheets)):
                    current_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[i], header=None)
                    title = clean_text(current_sheet.iloc[0, 0])

                    if 'tongsanphamtrongnuoctheogiahienhanh' in title and gdp_hh_sheet is None:
                        gdp_hh_sheet = current_sheet
                        continue
                    if 'tongsanphamtrongnuoctheogiasosanh' in title and gdp_ss_sheet is None: 
                        gdp_ss_sheet = current_sheet
                        break
                
                # Sửa lỗi Logic: Đưa kiểm tra None ra ngoài vòng For để tránh lỗi văng sớm
                if gdp_hh_sheet is None or gdp_ss_sheet is None:
                    print(f"Không xác định được sheet báo cáo GDP trong file báo cáo năm: {year}, tháng: {month} =========")   
                    return

                unit = 'Tỷ đồng'
                gdp_hh_sheet = gdp_hh_sheet.iloc[:, [1, 3]]
                gdp_ss_sheet = gdp_ss_sheet.iloc[:, [1, 3]]
                
                column_names = ['sector_and_sub_sector', 'value']
                gdp_hh_sheet.columns = column_names
                gdp_ss_sheet.columns = column_names
                
                num_of_row_del = 0
                sector_column = gdp_hh_sheet['sector_and_sub_sector']
                for row in sector_column:
                    if isinstance(row, str): break
                    num_of_row_del += 1
                gdp_hh_sheet = gdp_hh_sheet.iloc[num_of_row_del:, :].reset_index(drop=True)
                
                num_of_row_del = 0
                sector_column = gdp_ss_sheet['sector_and_sub_sector']
                for row in sector_column:
                    if isinstance(row, str): break
                    num_of_row_del += 1
                gdp_ss_sheet = gdp_ss_sheet.iloc[num_of_row_del:, :].reset_index(drop=True)
                
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
                
                gdp_hh_sheet['sector'] = gdp_hh_sheet['sector_and_sub_sector'].where(gdp_hh_sheet['sector_and_sub_sector'].isin(sectors))
                gdp_hh_sheet['sector'] = gdp_hh_sheet['sector'].ffill()
                gdp_ss_sheet['sector'] = gdp_ss_sheet['sector_and_sub_sector'].where(gdp_ss_sheet['sector_and_sub_sector'].isin(sectors))
                gdp_ss_sheet['sector'] = gdp_ss_sheet['sector'].ffill()
                
                gdp_ss_sheet['sub_sector'] = gdp_ss_sheet['sector_and_sub_sector']
                gdp_ss_sheet = gdp_ss_sheet[gdp_ss_sheet['sector'] != gdp_ss_sheet['sub_sector']]
                gdp_hh_sheet['sub_sector'] = gdp_hh_sheet['sector_and_sub_sector']
                gdp_hh_sheet = gdp_hh_sheet[gdp_hh_sheet['sector'] != gdp_hh_sheet['sub_sector']]

                gdp_ss_sheet = gdp_ss_sheet[gdp_ss_sheet['sub_sector'] != 'Công nghiệp']
                gdp_hh_sheet = gdp_hh_sheet[gdp_hh_sheet['sub_sector'] != 'Công nghiệp']

                # CHUẨN HÓA DỮ LIỆU SUB_SECTOR TRƯỚC KHI INSERT SILVER
                gdp_ss_sheet = clean_sub_sector(gdp_ss_sheet).reset_index(drop=True)
                gdp_hh_sheet = clean_sub_sector(gdp_hh_sheet).reset_index(drop=True)

                gdp_hh_sheet['value'] = pd.to_numeric(gdp_hh_sheet['value'], errors= 'coerce').round(3)
                gdp_ss_sheet['value'] = pd.to_numeric(gdp_ss_sheet['value'], errors= 'coerce').round(3)
                
                gdp_hh_sheet = gdp_hh_sheet.dropna()
                gdp_hh_sheet = gdp_hh_sheet.drop_duplicates()
                
                gdp_ss_sheet = gdp_ss_sheet.drop_duplicates()
                gdp_ss_sheet = gdp_ss_sheet.dropna()
                
                insert_df_to_table_silver_layer(gdp_ss_sheet, 'gdp', year, quarter)
                insert_df_to_table_silver_layer(gdp_hh_sheet, 'gdp', year, quarter)

            # -----------------------------------------------------------------
            # TRƯỜNG HỢP 3: Giai đoạn trước 2018 Quý 4
            # -----------------------------------------------------------------
            else: 
                gdp_hh_sheet = None
                gdp_ss_sheet = None
                
                for i in range(len(all_sheets)):
                    current_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[i], header=None)
                    title = clean_text(current_sheet.iloc[0, 0])

                    if 'tongsanphamtrongnuoctheogiahienhanh' in title and gdp_hh_sheet is None:
                        gdp_hh_sheet = current_sheet
                        continue
                    if 'tongsanphamtrongnuoctheogiasosanh' in title and gdp_ss_sheet is None: 
                        gdp_ss_sheet = current_sheet
                        break
                    
                if gdp_hh_sheet is None or gdp_ss_sheet is None:
                    print(f"Không xác định được sheet báo cáo GDP trong file báo cáo năm: {year}, tháng: {month} =========")   
                    return

                unit = 'Tỷ đồng'
                gdp_hh_sheet = gdp_hh_sheet.iloc[:, [0, 1, 3]]
                gdp_ss_sheet = gdp_ss_sheet.iloc[:, [0, 1, 3]]
                
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
                        
                gdp_hh_sheet = gdp_hh_sheet.iloc[num_of_remove_row_hh:, :].reset_index(drop=True)
                gdp_ss_sheet = gdp_ss_sheet.iloc[num_of_remove_row_ss:, :].reset_index(drop=True)

                column_names = ['sector', 'sub_sector', 'value']
                gdp_hh_sheet.columns = column_names
                gdp_ss_sheet.columns = column_names

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
                gdp_ss_sheet = gdp_ss_sheet.dropna().reset_index(drop=True)

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
                gdp_hh_sheet = gdp_hh_sheet.dropna().reset_index(drop=True)

                gdp_hh_sheet['quarter'] = gdp_ss_sheet['quarter'] = quarter
                gdp_hh_sheet['year'] = gdp_ss_sheet['year'] = year

                gdp_hh_sheet['type'], gdp_ss_sheet['type'] = 'Giá trị hiện hành', 'Giá trị so sánh'
                gdp_hh_sheet['ingest_at'] = gdp_ss_sheet['ingest_at'] = pd.Timestamp.now()
                gdp_hh_sheet['unit'] = gdp_ss_sheet['unit'] = unit

                gdp_ss_sheet = gdp_ss_sheet[gdp_ss_sheet['sub_sector'] != 'Công nghiệp']
                gdp_hh_sheet = gdp_hh_sheet[gdp_hh_sheet['sub_sector'] != 'Công nghiệp']

                # CHUẨN HÓA DỮ LIỆU SUB_SECTOR TRƯỚC KHI INSERT SILVER
                gdp_hh_sheet = clean_sub_sector(gdp_hh_sheet).reset_index(drop=True)
                gdp_ss_sheet = clean_sub_sector(gdp_ss_sheet).reset_index(drop=True)

                gdp_hh_sheet = gdp_hh_sheet.dropna()
                gdp_hh_sheet = gdp_hh_sheet.drop_duplicates()
                
                gdp_ss_sheet = gdp_ss_sheet.drop_duplicates()
                gdp_ss_sheet = gdp_ss_sheet.dropna()
                
                insert_df_to_table_silver_layer(gdp_hh_sheet, 'gdp', year, quarter)
                insert_df_to_table_silver_layer(gdp_ss_sheet, 'gdp', year, quarter)
                
    except Exception as e:
        print(f'CÓ VẤN ĐỀ XẢY RA KHI TRÍCH XUẤT DỮ LIỆU GDP {year}, THÁNG {month}', e)