import pandas as pd
import gc

from code_scripts.silver.numeric_data.minio_funcs import *
from code_scripts.silver.numeric_data.reuse_function import *
from code_scripts.silver.numeric_data.Load_data_to_table import *

from code_scripts.silver.numeric_data.gdp_extractor import extract_data_from_GDP
from code_scripts.silver.numeric_data.international_ecommerce_extractor import extract_data_from_International_Ecommerce
from code_scripts.silver.numeric_data.investment_extractor import extract_data_from_Invesment
from code_scripts.silver.numeric_data.investment_by_sector_extractor import extract_data_from_Investment_by_Sector
from code_scripts.silver.numeric_data.product_productivity_extractor import extract_data_for_Product_Productivity_fact
from code_scripts.silver.numeric_data.investment_by_sector_extractor import extract_data_from_Investment_by_Sector
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

        if excel_file is None:
            print('Đọc file Excel không thành công')
            continue
        
        print('duyệt qua từng đường dẫn đọc file và trích xuất dữ liệu')

        print(f'FILE EXCEL: YEAR : {year}, MONTH = {month} ')

        extract_data_from_GDP(excel_file, year, month)

        extract_data_from_International_Ecommerce(excel_file, year, month)

        if year != 2014 and month != 3: extract_data_from_Invesment(excel_file, year, month)

        extract_data_for_Product_Productivity_fact(excel_file, year, month)

        # Giải phóng bộ nhớ RAM của file hiện tại trước khi xử lý file tiếp theo
      
    print(f"Tải thành công dữ liệu từ file: tháng: {month} - năm: {year} lên SILVER LAYER")
    
    print('BẮT ĐẦU TRÍCH XUẤT DỮ LIỆU INVESTMENT BY SECTOR')
    excel_file = get_investment_by_sector_raw_data()
    extract_data_from_Investment_by_Sector(excel_file)

main_func()
