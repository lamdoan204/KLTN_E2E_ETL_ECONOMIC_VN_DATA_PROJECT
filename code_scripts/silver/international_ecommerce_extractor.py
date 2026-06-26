import pyspark.pandas as pd

from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *

# HÀM MỚI: CHUẨN HÓA VÀ MAPPING PRODUCT_NAME THEO YÊU CẦU
def clean_and_mapping_products(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    # 1. Xóa các khoảng trắng thừa ở đầu/cuối của product_name để đảm bảo mapping chính xác
    df['product_name'] = df['product_name'].astype(str).str.strip()


    # 2. Xóa các hàng có product_name = '-1' hoặc 'Tđ: Nguyên chiếc'
    df = df[~df['product_name'].isin(['-1', 'Tđ: Nguyên chiếc'])]
    df = df[~df['value'].isin([-1])]

    # 3. Tạo dictionary mapping cho các tên sản phẩm (Không bao gồm ô tô nguyên chiếc cần xử lý riêng)
    product_mapping = {
        'Đá quý, KL quý  và sản phẩm': 'Đá quý, kim loại quý và sản phẩm',
        'Điện thoại các loại và LK': 'Điện thoại các loại và linh kiện',
        'Điện thoại và linh kiện': 'Điện thoại các loại và linh kiện',
        'Điện thoại và LK': 'Điện thoại các loại và linh kiện',
        'Điện tử, máy tính và LK': 'Điện tử, máy tính và linh kiện',
        'Giấy các loại': 'Giấy và các sản phẩm từ giấy',
        'Gỗ và NPL gỗ': 'Gỗ và sản phẩm gỗ',
        'Hàng điện gia dụng và LK': 'Hàng điện gia dụng và linh kiện',
        'Hóa chất và sản phẩm hóa chất': 'Hóa chất',
        'Kim loại thường khác': 'Kim loại thường',
        'Kim loại thường khác và sản phẩm': 'Kim loại thường và sản phẩm',
        'Kim loại thường khác và SP': 'Kim loại thường và sản phẩm',
        'Máy ảnh, máy quay phim và LK': 'Máy ảnh, máy quay phim và linh kiện',
        'Máy móc, thiết bị, DC, PT khác' : 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Máy móc, thiết bị, dụng cụ PT khác' : 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Máy móc thiết bị, dụng cụ phụ tùng khác': 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Máy móc thiết bị, DC, PT khác': 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Máy móc thiết bị, DC PT khác' : 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Máy móc thiết bị, DC PT' : 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Máy móc thiết bị, dụng cụ PT': 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Máy móc thiết bị, dụng cụ PT khác': 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Máy móc, thiết bị, dụng cụ, phụ tùng khác': 'Máy móc, thiết bị, dụng cụ, phụ tùng',
        'Nguyên phụ liệu dệt may, da giầy': 'Nguyên phụ liệu dệt, may, giày dép',
        'Nguyên phụ liệu dệt, may, da, giày': 'Nguyên phụ liệu dệt, may, giày dép',
        'Nguyên PL dệt, may, giày dép': 'Nguyên phụ liệu dệt, may, giày dép',
        'Ô tô(*)': 'Ô tô',
        'Phương tiện vận tải khác và phụ tùng': 'Phương tiện vận tải và phụ tùng',
        'Phương tiện vận tải khác và PT': 'Phương tiện vận tải và phụ tùng',
        'Sản phẩm chất dẻo': 'Sản phẩm từ chất dẻo',
        'SP hóa chất': 'Sản phẩm hóa chất',
        'SP nội thất từ chất liệu khác gỗ': 'Sản phẩm nội thất từ chất liệu khác gỗ',
        'SP từ kim loại thường khác': 'Sản phẩm từ kim loại thường khác',
        'Thủy tinh và các SP từ thủy tinh': 'Thủy tinh và các sản phẩm từ thủy tinh',
        'Thủy tinh và cácSP từ thủy tinh' : 'Thủy tinh và các sản phẩm từ thủy tinh',
        'Thức ăn gia súc và NPL': 'Thức ăn gia súc và nguyên phụ liệu',
        'Xe máy(*)': 'Xe máy',
        'Sữa và sản phẩm sữa' : 'Sữa và sản phẩm từ sữa',
    }

    # Apply mapping chuẩn hóa tên sản phẩm chung
    df['product_name'] = df['product_name'].replace(product_mapping)

    # 4. Xử lý các trường hợp "Ô tô nguyên chiếc" và chuyển quantity_unit sang 'Chiếc'
    target_cars = ['Ô tô nguyên chiếc', 'Trong đó: Nguyên chiếc', 'Trong đó: Nguyên chiếc⁽*⁾', 'Trong đó: Nguyên chiếc(*)']
    
    # Cập nhật quantity_unit thành 'Chiếc' cho các dòng thỏa mãn điều kiện ô tô nguyên chiếc
    df.loc[df['product_name'].isin(target_cars), 'quantity_unit'] = 'Chiếc'
    # Đồng bộ tất cả các dòng này về một tên duy nhất: 'Ô tô nguyên chiếc'
    df['product_name'] = df['product_name'].replace({
        'Trong đó: Nguyên chiếc': 'Ô tô nguyên chiếc',
        ' Trong đó: Nguyên chiếc' : 'Ô tô nguyên chiếc',
        ' Trong đó: Nguyên chiếc⁽*⁾':'Ô tô nguyên chiếc',
        ' Tđ: Nguyên chiếc' : 'Ô tô nguyên chiếc',
        ' Trong đó: Nguyên chiếc': 'Ô tô nguyên chiếc',
        'Trong đó: Nguyên chiếc⁽*⁾': 'Ô tô nguyên chiếc',
        'Trong đó: Nguyên chiếc(*)': 'Ô tô nguyên chiếc',
        'Trong đó: Nguyên chiếc(**)' : 'Ô tô nguyên chiếc',
    })
    return df.reset_index(drop=True)


# TRÍCH XUẤT DỮ LIỆU THƯƠNG MẠI QUỐC TẾ
def extract_intenational_ecommerce_data_sheet_02(sheet : pd.DataFrame, type : str, month: int, year : int):
    try:
        # xóa các row không cần thiết
        num_of_remove_row = 0
        for i in range(len(sheet)):
            num_of_remove_row += 1
            if isinstance(sheet.iloc[i, 0], str) and 'mathangchuyeu' in clean_text(sheet.iloc[i, 0]): break

        if type == 'Import':
            sheet = sheet.iloc[num_of_remove_row:len(sheet) - 1, ::].reset_index(drop =True)
            
        else: sheet = sheet.iloc[num_of_remove_row::, ::].reset_index(drop =True)

        name_colums = ['product_name', 'quantity', 'value']
        #xoa cac cot kh can thiet
        sheet = sheet.iloc[::, [1, 2, 3]]
        sheet.columns = name_colums

        if type == 'Import': 
            for i in range(len(sheet)):
                if 'oto' == clean_text(str(sheet.loc[i, 'product_name'])):
                    sheet.loc[i, 'product_name'] = 'Ô tô và linh kiện'
                if 'Trong đó: Nguyên chiếc(*)' in str(sheet.loc[i, 'product_name']) : # Bọc str để tránh lỗi dữ liệu lạ
                    sheet.loc[ i, 'product_name'] = 'Ô tô nguyên chiếc' 
                    
        sheet['type'] = type
        quantity_unit = 'Nghìn tấn'
        unit = 'Triệu USD'
        sheet['quantity_unit'] = quantity_unit
        sheet['unit'] = unit
        sheet['month'] = month
        sheet['quarter'] = int((month -1) / 3) + 1
        sheet['year'] = year
        sheet['ingest_at'] = pd.Timestamp.now()
        sheet['quantity'] = sheet['quantity'].fillna(-1)
        sheet.loc[sheet['quantity'] == -1 , 'quantity_unit'] = 'Not Available'  
        sheet = sheet.dropna()
        return sheet
    except Exception as e:
        print(f'CÓ VẤN ĐỀ XẢY RA KHI TRÍCH XUẤT DỮ LIỆU THƯƠNG MẠI QUỐC TÊ NĂM {year}, THÁNG {month}', e)

def extract_intenational_ecommerce_data_sheet_01(sheet : pd.DataFrame, type: str, month: int, year: int):
    try:
    # xóa các row không cần thiết
        num_of_remove_row = 0
        for i in range(len(sheet)):
            num_of_remove_row += 1
            if isinstance(sheet.iloc[i, 0], str) and 'mathangchuyeu' in clean_text(sheet.iloc[i, 0]): break

        if type == 'Import':
            sheet = sheet.iloc[num_of_remove_row:len(sheet) - 1, ::].reset_index(drop =True)
            
        else: sheet = sheet.iloc[num_of_remove_row::, ::].reset_index(drop =True)

        name_colums = ['product_name', 'quantity', 'value']
        # xoa cac cot kh can thiet
        sheet = sheet.iloc[::, [1, 5, 6]]
        sheet.columns = name_colums
        if type == 'import': sheet.loc[ 29, 'product_name'] = 'Ô tô-nguyên chiếc' 
        
        unit = 'Triệu USD'
        quantity_unit = 'Nghìn tấn'
        ingest_at = pd.Timestamp.now()
        quarter = int((month -1 )/ 3) + 1
        sheet['type'] = type
        sheet['unit'], sheet['quantity_unit'], sheet['month'], sheet['quarter'], sheet['year'], sheet['ingest_at'] = \
            unit, quantity_unit, month, quarter, year, ingest_at
        sheet['quantity'] = sheet['quantity'].fillna(-1)
        sheet.loc[sheet['quantity'] == -1 , 'quantity_unit'] = 'Not Available'  
        sheet = sheet.dropna()
        return sheet
    except Exception as e:
        print(f'CÓ VẤN ĐỀ XẢY RA KHI TRÍCH XUẤT DỮ LIỆU THƯƠNG MẠI QUỐC TÊ NĂM {year}, THÁNG {month}', e)

def extract_data_from_International_Ecommerce(excel_file: pd.ExcelFile, year, month):
    all_sheets = excel_file.sheet_names
    import_sheet = None
    export_sheet = None
    
    # code xác định sheet báo cáo dữ liệu thương mại quốc tế
    for i in range(len(all_sheets)):
        sheet_name = clean_text(all_sheets[i])
        if any(name in sheet_name for name in ['nk', 'nhapkhau']) and all(name not in sheet_name for name in ['quy', 'gia']):
            import_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
        if any(name in sheet_name for name in ['xuatkhau', 'xk']) and all(name not in sheet_name for name in ['quy', 'gia']):
            export_sheet = pd.read_excel(excel_file, sheet_name= all_sheets[i], header= None)
        if import_sheet is not None and export_sheet is not None : break

    if year > 2018 or (year == 2018 and month >= 9) :
        import_sheet = extract_intenational_ecommerce_data_sheet_02(import_sheet, 'Import', month, year)
        export_sheet = extract_intenational_ecommerce_data_sheet_02(export_sheet, 'Export', month, year)
    else:
        import_sheet = extract_intenational_ecommerce_data_sheet_01(import_sheet, 'Import', month, year)
        export_sheet = extract_intenational_ecommerce_data_sheet_01(export_sheet, 'Export', month, year)
    
    
    # load lên silver với schema đã chuẩn hóa
    insert_df_to_table_silver_layer(import_sheet, 'international_ecommerce', year, month)
    insert_df_to_table_silver_layer(export_sheet, 'international_ecommerce', year, month)