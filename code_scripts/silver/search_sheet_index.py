import pandas as pd
from reuse_function import *


CONGNGHIEP_TITLE = 'motsosanphamchuyeucuanganhcongnghiep'
LAMNGHIEP_TITLE  = 'ketquasanxuatlamnghiep'
THUYSAN_TITLE    = 'sanluongthuysan'
CHANNUOI_TITLE   = 'sanphamchannuoi'
CHUYEU_TITLE     = 'sanluongmotsocaytrongchuyeu'
HANGNAM_TITLE    = 'sanluongmotsocaycongnghiephangnam'
LAUNAM_TITLE     = 'sanluongmotsocaycongnghieplaunam'


def search_start_and_end_index(title_sheet, sheet):
    """Tìm start_index và end_index của một bảng trong sheet."""
    start_index = -1
    end_index = len(sheet)

    title_list = [
        LAMNGHIEP_TITLE,
        THUYSAN_TITLE,
        CHANNUOI_TITLE,
        CHUYEU_TITLE,
        HANGNAM_TITLE,
        LAUNAM_TITLE,
    ]

    col_0 = sheet.iloc[::, 0]
    i = 0
    for row in col_0:
        if isinstance(row, str) and title_sheet in clean_text(row):
            start_index = i
            continue
        if (
            isinstance(row, str)
            and any(title in clean_text(row) for title in title_list if title is not title_sheet)
            and start_index != -1
        ):
            end_index = i + 1
            break
        i += 1

    return start_index, end_index


def find_sheet_indexes_monthly(excel_file, all_sheets):
    """Tìm sheet index cho tháng thường (không phải tháng cuối quý)."""
    san_pham_cong_nghiep_index = -1

    for i in range(len(all_sheets)):
        current_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[i], header=None)
        col_0 = current_sheet.iloc[::, 0]
        for row in range(len(col_0)):
            if isinstance(col_0[row], str):
                cleaned_text = clean_text(col_0[row])
                if CONGNGHIEP_TITLE == cleaned_text and san_pham_cong_nghiep_index == -1:
                    san_pham_cong_nghiep_index = i
        if san_pham_cong_nghiep_index != -1:
            break

    return san_pham_cong_nghiep_index


def find_sheet_indexes_quarterly(excel_file, all_sheets):
    """Tìm sheet index cho tháng cuối quý (tháng 3, 6, 9, 12)."""
    san_pham_cong_nghiep_index = -1
    lam_nghiep_sheet_index     = -1
    thuy_san_sheet_index       = -1
    chan_nuoi_sheet_index      = -1

    for i in range(len(all_sheets)):
        current_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[i], header=None)
        col_0 = current_sheet.iloc[::, 0]
        for row in range(len(col_0)):
            if isinstance(col_0[row], str):
                cleaned_text = clean_text(col_0[row])
                if 'sanluongthuysan'   in cleaned_text and thuy_san_sheet_index  == -1: thuy_san_sheet_index  = i
                if 'sanxuatlamnghiep' in cleaned_text and lam_nghiep_sheet_index == -1: lam_nghiep_sheet_index = i
                if 'channuoi'          in cleaned_text and chan_nuoi_sheet_index  == -1: chan_nuoi_sheet_index  = i
                if CONGNGHIEP_TITLE   == cleaned_text and san_pham_cong_nghiep_index == -1: san_pham_cong_nghiep_index = i
        print(san_pham_cong_nghiep_index)
        if all(index != -1 for index in [lam_nghiep_sheet_index, thuy_san_sheet_index,
                                          chan_nuoi_sheet_index, san_pham_cong_nghiep_index]):
            break

    return san_pham_cong_nghiep_index, lam_nghiep_sheet_index, thuy_san_sheet_index, chan_nuoi_sheet_index


def find_sheet_indexes_annual(excel_file, all_sheets, year):
    """Tìm sheet index cho tháng 12 (cây hằng năm, cây lâu năm, cây chủ yếu)."""
    cay_hang_nam_sheet_index    = -1
    cay_lau_nam_sheet_index     = -1
    cay_trong_chu_yeu_sheet_index = -1
    
    for i in range(len(all_sheets)):
        if year >2022 and year < 2025:
            i += 1
        current_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[i], header=None)
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
        if all(sheet_index != -1 for sheet_index in [cay_hang_nam_sheet_index,
                                                       cay_trong_chu_yeu_sheet_index,
                                                       cay_lau_nam_sheet_index]):
            break

    return cay_hang_nam_sheet_index, cay_lau_nam_sheet_index, cay_trong_chu_yeu_sheet_index
