import logging
import re

import numpy as np
import pandas as pd

from Load_data_to_table import insert_df_to_table_silver_layer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bảng chuẩn hóa đơn vị
# ---------------------------------------------------------------------------
UNIT_ALIASES: dict[str, str] = {
    "nghìntấn":      "Nghìn tấn",
    "1000tấn":       "Nghìn tấn",
    "nghìnchiếc":    "Nghìn chiếc",
    "1000chiếc":     "Nghìn chiếc",
    "nghìncái":      "Nghìn cái",
    "1000cái":       "Nghìn cái",
    "tỷkwh":         "Tỷ kWh",
    "nghìntỷđồng":  "Nghìn tỷ đồng",
}

# ---------------------------------------------------------------------------
# Bảng chuẩn hóa tên sản phẩm
# KEY đã được lowercase sẵn để so sánh trực tiếp với match_key
# ---------------------------------------------------------------------------
PRODUCT_MAPPING: dict[str, str] = {
    "dầu mỏ thô khai thác trong nước":                        "Dầu mỏ thô khai thác",
    "dầu thô khai thác":                                      "Dầu mỏ thô khai thác",
    "giày, dép, ủng bằng da giả cho người lớn":              "Giày, dép da",
    "giày, dép, ủng bằng da giả":                            "Giày, dép da",
    "phân hỗn hợp (n, p, k)":                                "Phân hỗn hợp N.P.K",
    "phân u rê":                                              "Phân Ure",
    "sơn hoá học các loại":                                   "Sơn hóa học",
    "sx trang phục (trừ quần áo da lông thú)":               "Sản xuất trang phục (trừ quần áo da lông thú)",
    "than sạch":                                              "Than đá (than sạch)",
    "ti vi":                                                  "Tivi",
    "tivi các loại":                                          "Tivi",
    "ti vi các loại":                                         "Tivi",
    "vải dệt từ sợi bông":                                    "Vải dệt từ sợi tự nhiên",
    "xăng dầu":                                               "Xăng, dầu",
    "xăng, dầu các loại":                                     "Xăng, dầu",
}

# ---------------------------------------------------------------------------
# Danh sách tên sản phẩm cần xóa hoàn toàn (đã lowercase, dùng set để O(1))
#
# CHÚ Ý: Đây chỉ là các dòng KHÔNG mang thông tin sản phẩm cụ thể (dòng tổng,
# dòng tiêu đề nhóm không có số liệu riêng). KHÔNG đưa các chuỗi ghép kiểu
# "chia ra: <tên chi tiết>" hoặc "trong đó: <tên chi tiết>" vào đây — các dòng
# đó cần được GIỮ LẠI, chỉ bóc tiền tố "Chia ra:"/"Trong đó:" (xem hàm
# _strip_breakdown_prefix), không xóa cả dòng.
# ---------------------------------------------------------------------------
PRODUCTS_TO_DELETE: set[str] = {
    "bia các loại - trong đó",
    "thép tròn các loại - chia ra",
    'chia ra:',
    'trong đó',
    'bia các loại', 
}

# ---------------------------------------------------------------------------
# Tiền tố dòng "chia nhỏ" cần bóc bỏ, giữ lại phần tên chi tiết phía sau.
# Ví dụ: "Chia ra: Thép tròn 8mm trở xuống" -> "Thép tròn 8mm trở xuống"
#        "Trong đó: Bia hơi"                -> "Bia hơi"
# ---------------------------------------------------------------------------
BREAKDOWN_PREFIX_PATTERN = re.compile(r"^(chia ra|trong đó)\s*:\s*", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_unit(unit: str | None) -> str | None:
    """Chuẩn hóa chuỗi đơn vị đo lường.

    Bước A – Xóa ký tự xuống dòng, thu gọn khoảng trắng thừa.
    Bước B – Tạo key tìm kiếm: viết thường + bỏ toàn bộ khoảng trắng.
    Bước C – Tra cứu UNIT_ALIASES; không tìm thấy thì giữ chuỗi đã làm sạch.
    """
    if unit is None:
        return unit

    # Bước A
    cleaned = re.sub(r"\s+", " ", str(unit).replace("\n", " ")).strip()

    # Bước B
    key = re.sub(r"\s+", "", cleaned).lower()

    # Bước C
    return UNIT_ALIASES.get(key, cleaned)


def _strip_breakdown_prefix(text: str) -> str:
    """Bóc bỏ tiền tố 'Chia ra:' / 'Trong đó:' (không phân biệt hoa thường),
    giữ lại phần tên sản phẩm chi tiết phía sau.

    Ví dụ:
        "Chia ra: Thép tròn 8mm trở xuống" -> "Thép tròn 8mm trở xuống"
        "Trong đó: Bia hơi"                -> "Bia hơi"
        "Thép tròn 10mm trở lên"           -> "Thép tròn 10mm trở lên" (không đổi)
    """
    return BREAKDOWN_PREFIX_PATTERN.sub("", text).strip()


def _normalize_product_name(name: object) -> str | None:
    """Chuẩn hóa tên sản phẩm.

    Trả về None nếu:
    - Giá trị rỗng / NaN.
    - Tên (sau khi đã bóc tiền tố "Chia ra:"/"Trong đó:") nằm trong
      PRODUCTS_TO_DELETE.

    Trả về tên đã được chuẩn hóa theo PRODUCT_MAPPING (hoặc giữ nguyên
    nếu không có trong bảng).
    """
    if pd.isna(name):
        return None

    # 1. Làm sạch khoảng trắng / xuống dòng
    cleaned: str = re.sub(r"\s+", " ", str(name).replace("\n", " ")).strip()
    if not cleaned:
        return None

    # 2. Bóc tiền tố "Chia ra:" / "Trong đó:" — GIỮ LẠI phần tên chi tiết
    #    phía sau, không xóa cả dòng.
    cleaned = _strip_breakdown_prefix(cleaned)
    if not cleaned:
        return None

    # 3. Key lowercase để kiểm tra danh sách đen và tra bảng mapping
    match_key: str = cleaned.lower()

    # 4. Kiểm tra danh sách cần xóa (lowercase cả hai vế)
    if match_key in PRODUCTS_TO_DELETE:
        return None

    # 5. Sửa viết tắt cũ
    cleaned = cleaned.replace("sợi TH", "sợi tổng hợp")
    # Đồng bộ lại match_key sau khi sửa viết tắt
    match_key = cleaned.lower()

    # 6. Tra bảng PRODUCT_MAPPING (key trong dict đã lowercase sẵn)
    if match_key in PRODUCT_MAPPING:
        return PRODUCT_MAPPING[match_key]

    # 7. Thử lại sau khi bỏ đuôi " các loại"
    match_key_no_kat = re.sub(r"\s+các loại$", "", match_key).strip()
    if match_key_no_kat in PRODUCT_MAPPING:
        return PRODUCT_MAPPING[match_key_no_kat]

    return cleaned


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

def extract_industry_product(
    sheet: pd.DataFrame,
    month: int,
    year: int,
) -> pd.DataFrame:
    """Trích xuất dữ liệu sản phẩm ngành công nghiệp từ một sheet Excel.

    Args:
        sheet: DataFrame thô đọc từ Excel (header=None).
        month: Tháng của dữ liệu (1–12).
        year:  Năm của dữ liệu.

    Returns:
        DataFrame đã làm sạch với các cột:
        product_name, unit, value, month, quarter, year, ingest_at.

    Raises:
        ValueError: Nếu sheet có ít hơn 4 cột.
    """
    sheet = sheet.copy()

    # --- 1. Kiểm tra số cột tối thiểu ------------------------------------
    EXPECTED_MIN_COLS = 4
    if sheet.shape[1] < EXPECTED_MIN_COLS:
        raise ValueError(
            f"Sheet tháng {month}/{year} chỉ có {sheet.shape[1]} cột "
            f"(cần ít nhất {EXPECTED_MIN_COLS})."
        )

    # --- 2. Chọn cột: cột 0 (tên), cột 1 (đơn vị), cột 3 (giá trị) -----
    sheet = sheet.iloc[:, [0, 1, 3]].copy()
    sheet.columns = ["product_name", "unit", "value"]

    # --- 3. Forward-fill đơn vị bị merge (ký hiệu '"') ------------------
    sheet["unit"] = (
        sheet["unit"]
        .astype(str)
        .str.replace("\n", " ", regex=False)
    )
    quote_mask = sheet["unit"].str.strip() == '"'
    sheet.loc[quote_mask, "unit"] = np.nan
    sheet["unit"] = sheet["unit"].ffill()

    # --- 4. Chuẩn hóa tên sản phẩm -------------------------------------
    sheet["product_name"] = sheet["product_name"].apply(_normalize_product_name)

    # --- 5. Loại dòng thiếu tên hoặc giá trị ---------------------------
    sheet = sheet.dropna(subset=["product_name", "value"]).reset_index(drop=True)

    # --- 6. Chuẩn hóa đơn vị -------------------------------------------
    sheet["unit"] = sheet["unit"].apply(_normalize_unit)

    # --- 7. Ép kiểu giá trị, loại dòng không hợp lệ --------------------
    sheet["value"] = pd.to_numeric(sheet["value"], errors="coerce")
    sheet = sheet.dropna(subset=["value"]).reset_index(drop=True)

    # --- 8. Gắn metadata thời gian --------------------------------------
    sheet["month"]     = month
    sheet["quarter"]   = (month - 1) // 3 + 1
    sheet["year"]      = year
    sheet["ingest_at"] = pd.Timestamp.now()

    logger.info(
        "extract_industry_product: tháng %d/%d → %d dòng hợp lệ.",
        month, year, len(sheet),
    )
    return sheet


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

def insert_industry_product(
    excel_file,
    all_sheets: list[str],
    sheet_index: int,
    month: int,
    year: int,
) -> None:
    """Đọc sheet, trích xuất và insert dữ liệu sản phẩm công nghiệp.

    Args:
        excel_file:   Đường dẫn hoặc file-like object của file Excel.
        all_sheets:   Danh sách tên sheet trong file.
        sheet_index:  Vị trí của sheet cần xử lý trong all_sheets.
        month:        Tháng của dữ liệu.
        year:         Năm của dữ liệu.
    """
    quarter = (month - 1) // 3 + 1

    raw = pd.read_excel(
        excel_file,
        sheet_name=all_sheets[sheet_index],
        header=None,
    )

    df = extract_industry_product(raw, month=month, year=year)
    df = df.drop_duplicates()
    df = df.dropna()
    df['value'] = pd.to_numeric(df["value"], errors= 'coerce').round(3)
    insert_df_to_table_silver_layer(df, "industry_product", year, quarter)

    logger.info(
        "insert_industry_product: đã insert %d dòng cho tháng %d/%d (Q%d).",
        len(df), month, year, quarter,
    )