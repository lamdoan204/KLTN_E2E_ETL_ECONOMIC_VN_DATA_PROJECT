import pyspark.pandas as pd
import numpy as np

import unicodedata  #

from Load_data_to_table import *
from search_sheet_index import search_start_and_end_index, THUYSAN_TITLE


def normalize_product_name(series: pd.Series) -> pd.Series:
    """Chuẩn hóa text: ép về Unicode NFC + gộp các biến thể gõ dấu khác nhau
    (vd: 'Thuỷ' / 'Thủy' đều thành 'Thủy')."""
    s = series.astype(str).str.strip()
    s = s.apply(lambda x: unicodedata.normalize('NFC', x))
    s = s.str.replace('Thuỷ', 'Thủy', regex=False)
    return s


def _clean_empty_to_nan(df: pd.DataFrame, cols) -> pd.DataFrame:
    """Chuẩn hóa các ô 'rỗng giả' (chuỗi rỗng, khoảng trắng, hoặc literal
    'nan'/'NaN'/'None'/'NaT' sinh ra từ astype(str) trên NaN) thành NaN thật,
    để dropna() có thể loại bỏ đúng. Bắt buộc gọi hàm này SAU khi
    normalize_product_name() và TRƯỚC khi dropna()/insert.
    """
    empty_like = {'', 'nan', 'NaN', 'None', 'NaT', 'none', 'null', 'NULL'}
    for c in cols:
        df[c] = df[c].apply(lambda x: np.nan if (x is None or str(x).strip() in empty_like) else x)
    return df


def _extract_annual_layout(sheet, value_col: int, unit: str):
    """Trích xuất dữ liệu thủy sản từ layout báo cáo tháng 12, nơi cột 0 chứa
    cả tên nhóm ('Nuôi trồng'/'Khai thác') và tên sản phẩm ('Cá','Tôm',...),
    còn các cột số liệu (Q3 / Q4 / cả năm) nằm ở các cột tiếp theo.

    value_col: chỉ số cột chứa giá trị quý cần lấy (cột Q3 hoặc cột Q4).
    """
    col0 = sheet.iloc[:, 0].astype(str).str.strip()
    start_idx = col0[col0 == 'Tổng số'].index
    if len(start_idx) == 0:
        raise ValueError(
            "Không tìm thấy dòng 'Tổng số' trong sheet thủy sản layout báo cáo năm."
        )
    # Bỏ dòng 'Tổng số' (đây là dòng tổng gộp Nuôi trồng + Khai thác, không insert).
    sub = sheet.iloc[start_idx[0] + 1:].copy()
    sub = sub.iloc[:, [0, value_col]]
    sub.columns = ['label', 'value']
    sub['label'] = sub['label'].astype(str).str.strip()
    sub = sub.dropna(subset=['value']).reset_index(drop=True)

    # --- Cắt bỏ các dòng tổng-theo-sản-phẩm (Cá/Tôm/Thủy sản khác cấp tổng)
    # nằm TRƯỚC dòng 'Nuôi trồng'. Đây là nguồn gốc của các dòng aquatic_type = NaN. ---
    idx = sub[sub['label'] == 'Nuôi trồng'].index
    if len(idx) > 0:
        sub = sub.loc[idx[0]:].reset_index(drop=True)
    # ------------------------------------------------------------------------------

    sub['aquatic_group'] = np.where(
        sub['label'].isin(['Nuôi trồng', 'Khai thác']), sub['label'], np.nan
    )
    sub['aquatic_group'] = sub['aquatic_group'].ffill()
    result = sub[~sub['label'].isin(['Nuôi trồng', 'Khai thác'])].copy()
    result['product_name'] = result['label']
    result['unit'] = unit
    return result[['aquatic_group', 'product_name', 'value', 'unit']].rename(
        columns={'aquatic_group': 'aquatic_type'}
    )

def insert_aquatic_products(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất và insert dữ liệu thủy sản."""
    thuysan_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    unit = 'Nghìn tấn'
    try:
        if thuysan_sheet.shape[1] < 5:
            # -- Định dạng ít cột --
            start_index, end_index = search_start_and_end_index(THUYSAN_TITLE, thuysan_sheet)

            thuysan_sheet = (
                thuysan_sheet.iloc[start_index:end_index, [0, 2]]
                .dropna()
                .reset_index(drop=True)
            )
            thuysan_sheet.columns = ['aquatic_type', 'value']
            thuysan_sheet['unit'] = unit

            idx = thuysan_sheet[thuysan_sheet['aquatic_type'].str.strip() == 'Nuôi trồng'].index[0]
            thuysan_sheet = thuysan_sheet.loc[idx:].reset_index(drop=True)

            # đánh dấu header group
            thuysan_sheet['aquatic_group'] = np.where(
                thuysan_sheet['aquatic_type'].isin(['Nuôi trồng', 'Khai thác']),
                thuysan_sheet['aquatic_type'],
                np.nan,
            )
            thuysan_sheet['aquatic_group'] = thuysan_sheet['aquatic_group'].ffill()

            # loại bỏ dòng tổng
            result = thuysan_sheet[
                ~thuysan_sheet['aquatic_type'].isin(['Nuôi trồng', 'Khai thác'])
            ].copy()

            result['product_name'] = result['aquatic_type']
            result['aquatic_type'] = result['aquatic_group']

            thuysan_sheet = result[['aquatic_type', 'product_name', 'value', 'unit']]

        else:
            # -- Định dạng nhiều cột --
            start_index, end_index = search_start_and_end_index(THUYSAN_TITLE, thuysan_sheet)

            df = thuysan_sheet.iloc[start_index:end_index].copy()

            df = df[[1, 2, 4]].copy()
            df.columns = ['aquatic_group', 'product_name', 'value']

            df['aquatic_group'] = df['aquatic_group'].ffill()
            df = df.dropna(subset=['product_name'])

            idx = df[df['aquatic_group'].str.strip() == 'Nuôi trồng'].index[0]
            df = df.loc[idx:].reset_index(drop=True)

            result = df[~df['product_name'].isin(['Nuôi trồng', 'Khai thác'])].copy()
            result['unit'] = unit

            thuysan_sheet = result[['aquatic_group', 'product_name', 'value', 'unit']].rename(
                columns={'aquatic_group': 'aquatic_type'}
            )
        thuysan_sheet['value'] = pd.to_numeric(thuysan_sheet['value'], errors= 'coerce').round(3)
        
        thuysan_sheet['quarter']   = quarter
        thuysan_sheet['year']      = year
        thuysan_sheet['ingest_at'] = pd.Timestamp.now()

        # Chuẩn hóa tên sản phẩm/nhóm (gộp biến thể Unicode "Thuỷ"/"Thủy").
        thuysan_sheet['product_name'] = normalize_product_name(thuysan_sheet['product_name'])
        thuysan_sheet['aquatic_type'] = normalize_product_name(thuysan_sheet['aquatic_type'])

        # QUAN TRỌNG: normalize_product_name() dùng astype(str) nên các ô vốn
        # là NaN/None/'' sẽ bị biến thành chuỗi literal 'nan'/'NaN'/'' chứ KHÔNG
        # còn là NaN thật -> dropna() phía dưới sẽ không bắt được. Phải đưa
        # chúng về lại NaN thật trước khi lọc/dropna.
        thuysan_sheet = _clean_empty_to_nan(thuysan_sheet, ['product_name', 'aquatic_type'])

        # Loại rõ các dòng group = NaN trước khi dropna() tổng quát.
        thuysan_sheet = thuysan_sheet[thuysan_sheet['aquatic_type'].notna()]
        thuysan_sheet = thuysan_sheet[thuysan_sheet['product_name'].notna()]
        thuysan_sheet = thuysan_sheet.dropna()
        
        thuysan_sheet['value'] = pd.to_numeric(thuysan_sheet['value'], errors= 'coerce').round(3)
        
        insert_df_to_table_silver_layer(thuysan_sheet, 'aquatic_products', year, quarter)
    except:
        # col_by_quarter: vị trí cột giá trị trong layout "cột 0 = label".
        # quarter 1 -> cột 1 (quý I); quarter 2 -> cột 2 (quý II, sheet 6 tháng);
        # quarter 3 -> cột 1 (sheet tháng 9, đã chạy ổn từ trước);
        # quarter 4 -> cột 2 (sheet tháng 12).
        col_by_quarter = {1: 1, 2: 2, 3: 1, 4: 2}
        value_col = col_by_quarter.get(quarter)
        if value_col is None:
            raise ValueError(
                f"Sheet thủy sản layout báo cáo năm không có cột cho quarter={quarter}; "
                f"chỉ hỗ trợ quarter 1, 2, 3 hoặc 4."
            )
        thuysan_sheet = _extract_annual_layout(thuysan_sheet, value_col=value_col, unit=unit)
        thuysan_sheet['quarter']   = quarter
        thuysan_sheet['year']      = year
        thuysan_sheet['ingest_at'] = pd.Timestamp.now()

        thuysan_sheet['product_name'] = normalize_product_name(thuysan_sheet['product_name'])
        thuysan_sheet['aquatic_type'] = normalize_product_name(thuysan_sheet['aquatic_type'])

        # Cùng lý do như nhánh try ở trên: đưa 'nan'/'' literal về lại NaN thật.
        thuysan_sheet = _clean_empty_to_nan(thuysan_sheet, ['product_name', 'aquatic_type'])

        thuysan_sheet = thuysan_sheet[thuysan_sheet['aquatic_type'].notna()]
        thuysan_sheet = thuysan_sheet[thuysan_sheet['product_name'].notna()]
        thuysan_sheet = thuysan_sheet.dropna()

        thuysan_sheet['value'] = pd.to_numeric(thuysan_sheet['value'], errors= 'coerce').round(3)

        insert_df_to_table_silver_layer(thuysan_sheet, 'aquatic_products', year, quarter)