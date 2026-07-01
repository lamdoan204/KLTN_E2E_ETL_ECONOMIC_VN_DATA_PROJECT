import pandas as pd
import re
from datetime import datetime, timezone
from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *


def extract_data_from_Investment_by_Sector(df: pd.ExcelFile):
    """
    Trích xuất dữ liệu 'Vốn đầu tư thực hiện toàn xã hội theo giá hiện hành
    phân theo ngành kinh tế' từ file Excel thành bảng investment_by_sector.
    Output columns: name (STRING), value (DOUBLE), unit (STRING),
                     year (INT), ingest_at (TIMESTAMP)
    """
    sheet_name = df.sheet_names[0]
    raw = df.parse(sheet_name, header=None)

    # Dòng chứa năm (header) là dòng có nhiều giá trị số dạng năm (2010, 2011, ...)
    header_row_idx = None
    for i in range(min(10, raw.shape[0])):
        row = raw.iloc[i, 1:]
        year_like = 0
        for v in row:
            if pd.isna(v):
                continue
            s = str(v)
            if re.search(r'(19|20)\d{2}', s):
                year_like += 1
        if year_like >= 3:
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError("Không tìm thấy dòng tiêu đề năm trong sheet")

    year_row = raw.iloc[header_row_idx, 1:]

    # Parse năm từ tiêu đề cột (loại bỏ text như 'Sơ bộ 2024' -> 2024)
    years = {}
    for col_idx, v in year_row.items():
        if pd.isna(v):
            continue
        m = re.search(r'(19|20)\d{2}', str(v))
        if m:
            years[col_idx] = int(m.group(0))

    # Bảng ánh xạ chuẩn hóa tên ngành
    # LƯU Ý: key phải viết ở dạng chữ thường (lowercase) vì clean_name()
    # tra cứu bằng key.lower(). Chuẩn hóa lại dict ngay bên dưới để tránh
    # lỗi mismatch case trong tương lai dù bạn viết key theo dạng nào.
    NAME_NORMALIZE_MAP = {
        'Hoạt động khác': 'Hoạt động dịch vụ khác',
    }
    NAME_NORMALIZE_MAP = {k.lower().strip(): v for k, v in NAME_NORMALIZE_MAP.items()}

    def clean_name(raw_name: str) -> str:
        if raw_name is None:
            return None
        s = str(raw_name)
        s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        s = re.sub(r'[^\S\r\n]+', ' ', s)   # gộp nhiều khoảng trắng thành 1
        s = re.sub(r'\s+', ' ', s).strip()
        s = s.replace(';', ',')
        # Loại bỏ số thứ tự / ký tự đầu dòng kiểu "1.", "1)", "-", "*"
        s = re.sub(r'^[\-\*\.\)\(\d\s]+(?=[A-ZĐÂÁÀẢÃẠÊÉÈẺẼẸÔÓÒỎÕỌƠỚỜỞỠỢƯỨỪỬỮỰÍÌỈĨỊÝỲỶỸỴ])', '', s)
        s = s.strip()

        key = s.lower().strip()
        if key in NAME_NORMALIZE_MAP:
            s = NAME_NORMALIZE_MAP[key]
        return s

    records = []
    ingest_at = datetime.now(timezone.utc)

    # Dữ liệu bắt đầu từ dòng ngay sau dòng header năm
    for i in range(header_row_idx + 1, raw.shape[0]):
        name_raw = raw.iloc[i, 0]
        if pd.isna(name_raw):
            continue
        name = clean_name(name_raw)
        if not name:
            continue

        # Bỏ dòng tổng số
        if name.strip().lower() in ('tổng số', 'tổng cộng'):
            continue

        for col_idx, year in years.items():
            val = raw.iloc[i, col_idx]
            if pd.isna(val):
                continue
            try:
                value = float(val)
            except (ValueError, TypeError):
                val_str = re.sub(r'[^\d\.\-]', '', str(val))
                if val_str in ('', '-', '.'):
                    continue
                try:
                    value = float(val_str)
                except ValueError:
                    continue

            records.append({
                'name': name,
                'value': value,
                'unit': 'Tỷ đồng',
                'year': year,
                'ingest_at': ingest_at,
            })

    result_df = pd.DataFrame(records, columns=['name', 'value', 'unit', 'year', 'ingest_at'])
    insert_df_to_table_silver_layer(result_df, 'investment_by_sector')


