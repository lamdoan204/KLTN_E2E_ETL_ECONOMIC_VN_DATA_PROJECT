import pyspark.pandas as pd
import numpy as np
from Load_data_to_table import *
from reuse_function import *
from search_sheet_index import search_start_and_end_index, CHUYEU_TITLE


def insert_staple_crops(excel_file, all_sheets, sheet_index: int, year: int, quarter: int):
    """Đọc sheet, trích xuất và insert dữ liệu cây trồng chủ yếu."""
    if sheet_index == -1:
        print('Không công bố dữ liệu về cây trồng chủ yếu')
        return

    caychuyeu_sheet = pd.read_excel(excel_file, sheet_name=all_sheets[sheet_index], header=None)
    start_index, end_index = search_start_and_end_index(CHUYEU_TITLE, caychuyeu_sheet)

    if year <= 2023:
        # Format cũ: nhãn cấp 1 (Lúa cả năm, Ngô...) ở cột 1, nhãn cấp 2 (vụ lúa con)
        # ở cột 2, giá trị ở cột 3. Gộp cột 1&2 thành 1 cột nhãn duy nhất.
        caychuyeu_df = (
            caychuyeu_sheet.iloc[start_index + 1:end_index, [0, 1, 2, 3]]
            .reset_index(drop=True)
        )
        caychuyeu_df[1] = caychuyeu_df[1].combine_first(caychuyeu_df[2])
        caychuyeu_df = caychuyeu_df[[0, 1, 3]]
        caychuyeu_df.columns = [0, 1, 2]
    else:
        # Format mới (>2023): tất cả nhãn (cả lúa cả năm và vụ lúa con) dồn vào cột 0,
        # giá trị "Ước tính năm {year}" nằm ở cột 2. Dựng lại cột nhãn từ cột 0.
        caychuyeu_df = (
            caychuyeu_sheet.iloc[start_index + 1:end_index, [0, 2]]
            .reset_index(drop=True)
        )
        caychuyeu_df.columns = [0, 2]
        caychuyeu_df.insert(1, 1, caychuyeu_df[0])
        caychuyeu_df = caychuyeu_df[[0, 1, 2]]

    # Chuẩn hóa chuỗi rỗng/space (' ') thành NaN thật, vì Excel dùng ' ' thay cho ô
    # trống ở một số dòng tên cây (Ngô, Sắn...), khiến ffill() nhận diện sai.
    caychuyeu_df[1] = caychuyeu_df[1].replace(r'^\s*$', np.nan, regex=True)
    caychuyeu_df[2] = caychuyeu_df[2].replace(r'^\s*$', np.nan, regex=True)

    # Tìm ranh giới cây có hạt / cây có củ
    start_index_1 = -1  # cây lương thực có hạt
    end_index_1   = -1
    start_index_2 = -1  # cây chất bột có củ
    col_0 = caychuyeu_df[0]
    # Chỉ coi là tiêu đề mục lớn nếu dòng bắt đầu bằng số + dấu chấm (vd "1.", "2.")
    # -> tránh nhận nhầm dòng "...có hạt (Nghìn tấn)" (phần nối của "Tổng sản lượng
    # lương thực có hạt") làm ranh giới, vì dòng đó cũng chứa cụm "cohat" sau khi
    # chuẩn hóa text, dù không phải tiêu đề mục.
    is_section_title = lambda s: isinstance(s, str) and re.match(r'^\d+\.', s.strip())
    for i, row in enumerate(col_0):
        if is_section_title(row) and 'cohat' in clean_text(row):
            start_index_1 = i
        if isinstance(caychuyeu_df.iloc[i, 1], str) and 'tongsanluong' in clean_text(caychuyeu_df.iloc[i, 1]):
            end_index_1 = i
        if is_section_title(row) and 'cocu' in clean_text(row) and start_index_1 != -1:
            start_index_2 = i

    column_name = ['product_and_infor', f'value_{year}']
    cohat_df = caychuyeu_df.iloc[start_index_1:end_index_1, 1:].dropna(subset=[1]).reset_index(drop=True)
    cocu_df  = caychuyeu_df.iloc[start_index_2:len(caychuyeu_df), 1:].dropna(subset=[1]).reset_index(drop=True)
    cohat_df.columns = column_name
    cocu_df.columns  = column_name

    # Chỉ giữ "Lúa cả năm" -> đổi tên thành "Lúa". Loại bỏ các vụ lúa con (đông xuân,
    # hè thu, thu đông, mùa...). Nhận diện bằng MẪU "bắt đầu bằng 'Lúa' nhưng khác
    # 'Lúa cả năm'" thay vì liệt kê tên cụ thể, vì cách viết tên vụ lúa con thay đổi
    # qua các năm (vd "Lúa hè thu" năm 2018 nhưng "Lúa hè thu + Lúa thu đông" năm 2015).
    value_col = f'value_{year}'
    crop_name_tmp = cohat_df['product_and_infor'].where(cohat_df[value_col].isna()).ffill()
    is_lua_con = crop_name_tmp.str.startswith('Lúa') & (crop_name_tmp != 'Lúa cả năm')
    cohat_df = cohat_df[~is_lua_con].reset_index(drop=True)
    cohat_df['product_and_infor'] = cohat_df['product_and_infor'].replace('Lúa cả năm', 'Lúa')

    cohat_df['unit'] = cohat_df['product_and_infor'].str.extract(r"\((.*?)\)").fillna(' ')
    cocu_df['unit']  = cocu_df['product_and_infor'].str.extract(r"\((.*?)\)").fillna(' ')

    def parse_crop_table(df, type_label):
        df = df.copy()
        value_col = f'value_{year}'
        df['crop_name'] = df['product_and_infor'].where(df[value_col].isna()).ffill()
        df = df[df[value_col].notna()].copy()
        df['metric'] = np.select(
            [
                df['product_and_infor'].str.contains('Diện tích'),
                df['product_and_infor'].str.contains('Năng suất'),
                df['product_and_infor'].str.contains('Sản lượng'),
            ],
            ['area', 'yield', 'production'],
            default='other',
        )
        values = df.pivot_table(index='crop_name', columns='metric', values=value_col,   aggfunc='first')
        units  = df.pivot_table(index='crop_name', columns='metric', values='unit',       aggfunc='first')
        units.columns = [f"{c}_unit" for c in units.columns]
        result = pd.concat([values, units], axis=1)
        result = result.rename(columns={
            'area':            'area',
            'yield':           'yield',
            'production':      'production',
            'area_unit':       'area_unit',
            'yield_unit':      'yield_unit',
            'production_unit': 'production_unit',
        })
        result['type'] = type_label
        result = result.reset_index()
        return result[['crop_name', 'type', 'area', 'area_unit', 'yield', 'yield_unit', 'production', 'production_unit']]

    merged_df = pd.concat(
        [
            parse_crop_table(cocu_df,  'Cây có củ'),
            parse_crop_table(cohat_df, 'Cây có hạt'),
        ],
        ignore_index=True,
    )
    merged_df['production'] = merged_df['production'].round(3)
    merged_df['yield'] = merged_df['yield'].round(3)
    merged_df['area'] = merged_df['area'].round(3)
    
    merged_df = merged_df.drop_duplicates()

    merged_df['year']      = year
    merged_df['ingest_at'] = pd.Timestamp.now()
    insert_df_to_table_silver_layer(merged_df, 'staple_crops', year, quarter)