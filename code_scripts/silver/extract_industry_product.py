import pandas as pd

from Load_data_to_table import *


import re
import logging
import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bảng chuẩn hóa đơn vị: gộp các biến thể viết khác nhau về 1 dạng chuẩn duy nhất.
# Khoá được so khớp sau khi đã lowercase + xoá khoảng trắng, nên thêm biến thể
# mới chỉ cần thêm 1 dòng vào đây, không cần sửa logic.
# ---------------------------------------------------------------------------
UNIT_ALIASES = {
    "nghìntấn": "Nghìn tấn",
    "1000tấn": "Nghìn tấn",
    "nghìnchiếc": "Nghìn chiếc",
    "1000chiếc": "Nghìn chiếc",
    "nghìncái": "Nghìn cái",
    "1000cái": "Nghìn cái",
    "tỷkwh": "Tỷ kWh",
    "nghìntỷđồng": "Nghìn tỷ đồng",
}


def _normalize_unit(unit: str) -> str:
    """Chuẩn hóa 1 chuỗi đơn vị về dạng chuẩn dựa trên UNIT_ALIASES.

    Nếu không khớp alias nào, trả về bản đã trim/strip khoảng trắng+xuống dòng
    (không đoán mò), để các trường hợp lạ vẫn được giữ lại y nguyên cho người
    review thủ công, thay vì bị "chuẩn hóa" sai.
    """
    if unit is None:
        return unit
    cleaned = re.sub(r"\s+", " ", str(unit).replace("\n", " ")).strip()
    key = re.sub(r"\s+", "", cleaned).lower()
    return UNIT_ALIASES.get(key, cleaned)


def _normalize_product_name(name: str) -> str:
    """Chuẩn hóa tên sản phẩm: gộp khoảng trắng, strip, và sửa các viết tắt
    đã biết gây trùng lặp (ví dụ 'sợi TH' vs 'sợi tổng hợp')."""
    cleaned = re.sub(r"\s+", " ", str(name).replace("\n", " ")).strip()
    # Các cặp viết tắt đã biết gây ra duplicate sản phẩm trong dữ liệu cũ.
    # Thêm cặp mới vào đây khi phát hiện case tương tự.
    cleaned = cleaned.replace("sợi TH", "sợi tổng hợp")
    return cleaned


def extract_industry_product(sheet: pd.DataFrame, month: int, year: int) -> pd.DataFrame:
    """Trích xuất dữ liệu sản phẩm ngành công nghiệp từ sheet.

    So với bản gốc, hàm này:
      1. Chọn cột theo TÊN (sau khi dò header) thay vì vị trí cố định,
         và cảnh báo/raise nếu không tìm thấy cột mong đợi.
      2. Forward-fill ô unit bị merge ('"') TRƯỚC khi dropna, để không mất
         dữ liệu hợp lệ và không forward-fill nhầm khi value đã bị loại.
      3. Chuẩn hóa unit và product_name về 1 dạng duy nhất.
      4. Validate value: convert numeric, loại NaN sau convert, cảnh báo
         giá trị âm hoặc outlier bất thường so với phần còn lại của sheet.
    """
    sheet = sheet.copy()

    # --- 1. Chọn cột theo tên thay vì vị trí cố định ---------------------
    # Giả định hàng đầu vẫn chứa header thô; nếu file gốc luôn có header=None
    # và cấu trúc 4 cột cố định [tên, đơn vị, mã số, giá trị], ta vẫn KIỂM TRA
    # explicit số cột trước khi slice, thay vì slice mù.
    expected_min_cols = 4
    if sheet.shape[1] < expected_min_cols:
        raise ValueError(
            f"Sheet tháng {month}/{year} chỉ có {sheet.shape[1]} cột, "
            f"cần tối thiểu {expected_min_cols}. Kiểm tra lại layout file Excel."
        )

    sheet = sheet.iloc[:, [0, 1, 3]].copy()
    sheet.columns = ['product_name', 'unit', 'value']

    # --- 2. Forward-fill unit bị merge ('"') TRƯỚC khi dropna -------------
    sheet['unit'] = sheet['unit'].astype(str)
    sheet['unit'] = sheet['unit'].str.replace('\n', ' ', regex=False)
    quote_mask = sheet['unit'].str.strip() == '"'
    # ffill xử lý luôn nhiều dòng '"' liên tiếp, không chỉ dòng ngay trước
    sheet.loc[quote_mask, 'unit'] = np.nan
    sheet['unit'] = sheet['unit'].ffill()

    # --- 3. Loại dòng rác (thiếu tên sản phẩm hoặc giá trị) ---------------
    sheet = sheet.dropna(subset=['product_name', 'value']).reset_index(drop=True)

    # --- 4. Chuẩn hóa product_name và unit --------------------------------
    sheet['product_name'] = sheet['product_name'].apply(_normalize_product_name)
    sheet['unit'] = sheet['unit'].apply(_normalize_unit)

    # --- 5. Validate value --------------------------------------------------
    sheet['value'] = pd.to_numeric(sheet['value'], errors='coerce')
    n_before = len(sheet)
    sheet = sheet.dropna(subset=['value']).reset_index(drop=True)
    n_dropped = n_before - len(sheet)
    if n_dropped:
        logger.warning(
            "Tháng %s/%s: loại %d dòng có value không convert được sang số.",
            month, year, n_dropped,
        )

    negative_mask = sheet['value'] < 0
    if negative_mask.any():
        logger.warning(
            "Tháng %s/%s: %d dòng có value âm: %s",
            month, year, negative_mask.sum(),
            sheet.loc[negative_mask, 'product_name'].tolist(),
        )

    # Cảnh báo outlier theo thống kê đơn giản (median * hệ số) để bắt sớm
    # các lỗi kiểu "Linh kiện điện thoại" (1 tháng tăng ~1000x rồi quay lại).
    # Đây chỉ là CẢNH BÁO, không tự sửa, vì có thể là biến động thật.
    if len(sheet) > 0:
        med = sheet['value'].median()
        if med > 0:
            outlier_mask = sheet['value'] > med * 100
            if outlier_mask.any():
                logger.warning(
                    "Tháng %s/%s: %d dòng giá trị bất thường (>100x median của sheet): %s",
                    month, year, outlier_mask.sum(),
                    list(zip(
                        sheet.loc[outlier_mask, 'product_name'],
                        sheet.loc[outlier_mask, 'value'],
                    )),
                )

    # --- 6. Gắn metadata thời gian ---------------------------------------
    sheet['month'] = month
    sheet['quarter'] = int((month - 1) / 3) + 1
    sheet['year'] = year
    sheet['ingest_at'] = pd.Timestamp.now()

    return sheet


def insert_industry_product(excel_file, all_sheets, sheet_index: int, month: int, year: int):
    """Đọc sheet, trích xuất và insert dữ liệu sản phẩm công nghiệp."""
    quarter = int((month - 1) / 3) + 1
    df = extract_industry_product(
        pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None),
        month=month,
        year=year,
    )
    insert_df_to_table_silver_layer(df, 'industry_product', year, quarter)
