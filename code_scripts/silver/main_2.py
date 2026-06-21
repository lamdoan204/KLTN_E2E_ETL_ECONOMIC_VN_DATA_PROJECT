import pyspark.pandas as pd
import gc

from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *

from gdp_extractor import extract_data_from_GDP
from international_ecommerce_extractor import extract_data_from_International_Ecommerce
from investment_extractor import extract_data_from_Invesment
from investment_by_sector_extractor import extract_data_from_Investment_by_Sector
from product_productivity_extractor import extract_data_for_Product_Productivity_fact

def main_func():
    # lấy tất cả các đường dẫn trong bronze
    bucket_name = 'bronze'
    src_prefix = 'newest_economic_report_excel_file/'
    dst_prefix = 'economic_report_excel_file/'

    objects = get_list_files(bucket_name, src_prefix)

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

        extract_data_from_Invesment(excel_file, year, month)

        extract_data_for_Product_Productivity_fact(excel_file, year, month)
        
        
        src_object_name  = obj
        dst_object_name = f"{dst_prefix}/{year}/{month}/{parts[3]}"
        
        copy_object_in_minio(bucket_name,src_object_name, dst_object_name )
        print('Chuyển sang Prefix chung thành công')
        
        # Giải phóng bộ nhớ RAM của file hiện tại trước khi xử lý file tiếp theo
        del excel_file
        spark.catalog.clearCache()
        gc.collect()
        
    print(f"Tải thành công dữ liệu từ file: tháng: {month} - năm: {year} lên SILVER LAYER")
   

main_func()
