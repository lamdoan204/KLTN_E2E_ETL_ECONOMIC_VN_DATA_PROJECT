import pyspark.pandas as pd

from code_scripts.silver.numeric_data.minio_funcs import *
from code_scripts.silver.numeric_data.reuse_function import *
from code_scripts.silver.numeric_data.search_sheet_index import (
    find_sheet_indexes_monthly,
    find_sheet_indexes_quarterly,
    find_sheet_indexes_annual,
)
from code_scripts.silver.numeric_data.extract_industry_product import insert_industry_product
from code_scripts.silver.numeric_data.extract_livestock import insert_livestock
from code_scripts.silver.numeric_data.extract_forestry import insert_forestry
from code_scripts.silver.numeric_data.extract_aquatic import insert_aquatic_products
from code_scripts.silver.numeric_data.extract_perennial_crops import insert_perennial_crops
from code_scripts.silver.numeric_data.extract_annual_crops import insert_annual_crops
from code_scripts.silver.numeric_data.extract_staple_crops import insert_staple_crops


# TRÍCH XUẤT DỮ LIỆU NĂNG SUẤT SẢN PHẨM - CÂY TRỒNG, VẬT NUÔI, LÂM NGHIỆP.
def extract_data_for_Product_Productivity_fact(excel_file: pd.ExcelFile, year: int, month: int):
    try:
        all_sheets = excel_file.sheet_names
        quarter = int((month - 1) / 3) + 1

        if month % 3 != 0:
            # ── Tháng thường: chỉ trích xuất sản phẩm công nghiệp ──────────────
            san_pham_cong_nghiep_index = find_sheet_indexes_monthly(excel_file, all_sheets)
            insert_industry_product(excel_file, all_sheets, san_pham_cong_nghiep_index, month, year)

        else:
            # ── Tháng cuối quý: trích xuất đầy đủ ──────────────────────────────
            (
                san_pham_cong_nghiep_index,
                lam_nghiep_sheet_index,
                thuy_san_sheet_index,
                chan_nuoi_sheet_index,
            ) = find_sheet_indexes_quarterly(excel_file, all_sheets)

            # Tháng 12: tìm thêm sheet cây trồng năm
            if month == 12:
            
                (
                    cay_hang_nam_sheet_index,
                    cay_lau_nam_sheet_index,
                    cay_trong_chu_yeu_sheet_index,
                ) = find_sheet_indexes_annual(excel_file, all_sheets, year)
                insert_perennial_crops(excel_file, all_sheets, cay_lau_nam_sheet_index,        year, quarter)       
                insert_annual_crops(   excel_file, all_sheets, cay_hang_nam_sheet_index,       year, quarter)
                insert_staple_crops(   excel_file, all_sheets, cay_trong_chu_yeu_sheet_index,  year, quarter)
            # ── Insert theo tháng ────────────────────────────────────────────────
            insert_industry_product(excel_file, all_sheets, san_pham_cong_nghiep_index, month, year)

            # ── Insert theo quý ──────────────────────────────────────────────────
            if chan_nuoi_sheet_index != -1 :
                insert_livestock(excel_file, all_sheets, chan_nuoi_sheet_index,  year, quarter)
            insert_forestry( excel_file, all_sheets, lam_nghiep_sheet_index, year, quarter)
            if year != 2017 and quarter != 1 or year != 2013 and quarter != 3:
                insert_aquatic_products(excel_file, all_sheets, thuy_san_sheet_index, year, quarter)

            

    except Exception as e:
        print(
            f'CÓ VẤN ĐỀ TRONG TRÍCH XUẤT DỮ LIỆU NĂNG SUẤT SẢN PHẨM '
            f'năm : {year}, tháng {month}',
            e,
        )
