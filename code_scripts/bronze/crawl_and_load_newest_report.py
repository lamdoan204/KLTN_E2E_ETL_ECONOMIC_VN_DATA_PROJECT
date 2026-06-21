import requests, os
from bs4 import BeautifulSoup
import unicodedata
import re
from minio import Minio
from reuse_function import *
from datetime import datetime, timezone, timedelta

client = Minio(
    'minio:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)


def create_bucket_if_not_exists(bucket_name):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully")
        else:
            print(f"Bucket '{bucket_name}' already exists")
    except Exception as e:
        print(f"Error creating bucket: {e}")


def load_file_to_Bronze(bucket_name, object_name, local_file_path):
    try:
        print(object_name, '\n', bucket_name, '\n', local_file_path)
        client.fput_object(
            bucket_name,
            object_name,
            local_file_path
        )
        print('SUCCESSFULLY LOAD DATA TO BRONZE LAYER !!!!!!!!!!!111')
    except Exception as e:
        print(f'HAVE AN ERROR WHEN LOAD FILE TO {bucket_name} !!!!!!!!!!')
        print(e)

def clear_prefix_in_minio(bucket_name, prefix):
        try:
            objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
            object_names = [obj.object_name for obj in objects]
    
            if not object_names:
                print(f'KHÔNG CÓ FILE NÀO TRONG {bucket_name}/{prefix} ĐỂ XÓA')
                return
    
            for object_name in object_names:
                client.remove_object(bucket_name, object_name)
                print(f'ĐÃ XÓA: {bucket_name}/{object_name}')
    
            print(f'ĐÃ DỌN SẠCH PREFIX {bucket_name}/{prefix}')
        except Exception as e:
            print(f'HAVE AN ERROR WHEN CLEAR PREFIX {bucket_name}/{prefix} !!!!!!!!!!')
            print(e)

def get_month_from_title(title):
    """
    Trích xuất tháng từ title bài báo cáo mới nhất (10 ký tự cuối thường là " - YYYY").
    title: chuỗi đã lowercase.
    """
    tmp_title = title[0:len(title) - 8]
    print('tmp title: ', tmp_title)

    month = None
    if any(e in tmp_title for e in ['1', 'một']):
        month = 1
    if any(e in tmp_title for e in ['2', 'hai']):
        month = 2
    if any(e in tmp_title for e in ['3', 'ba', 'quý i']):
        month = 3
    if any(e in tmp_title for e in ['4', 'bốn', 'tư']):
        month = 4
    if any(e in tmp_title for e in ['5', 'năm']):
        month = 5
    if any(e in tmp_title for e in ['6', 'sáu', 'quý ii']):
        month = 6
    if any(e in tmp_title for e in ['7', 'bảy']):
        month = 7
    if any(e in tmp_title for e in ['8', 'tám']):
        month = 8
    if any(e in tmp_title for e in ['9', 'chín', 'quý iii']):
        month = 9
    if any(e in tmp_title for e in ['10', 'mười']):
        month = 10
    if any(e in tmp_title for e in ['11', 'mười một']):
        month = 11
    if any(e in tmp_title for e in ['12', 'mười hai', 'quý iv']):
        month = 12

    return month


def craw_and_load_latest_report_economic_excel_file_to_bronze():
    """
    Chỉ lấy bài báo cáo kinh tế - xã hội MỚI NHẤT (bài đầu tiên trong archive-container),
    tải file Excel đính kèm và load lên MinIO Bronze với cấu trúc:
    economic_report_excel_files/{year}/{month}/{file_name}
    """
    base_url = 'https://www.nso.gov.vn/bao-cao-tinh-hinh-kinh-te-xa-hoi-hang-thang/'

    
    print('Lấy tên bài báo + link dẫn tới bài báo mới nhất................................')
    res = requests.get(base_url, verify=False)
    soup = BeautifulSoup(res.text, "html.parser")
    container = soup.find('div', class_='archive-container')

    the_a = container.find_all('a', class_=None)
    the_h3 = container.find_all('h3', class_=None)

    if not the_h3 or not the_a:
        print('KHÔNG TÌM THẤY BÀI BÁO CÁO NÀO TRONG archive-container !!!!!!!!!!')
        return

    # Bài đầu tiên = bài mới nhất
    title = str.lower(the_h3[0].get_text(strip=True))
    link = the_a[0]['href']

    print('Bài mới nhất:', title)
    print('Link:', link)

    if 'baocaotinhhinhkinhtexahoi' not in clean_text(title):
        print('Bài mới nhất không phải báo cáo kinh tế xã hội, dừng lại.')
        return

    # vào bài báo tải file excel về
    print('vào bài báo tải file excel về ===========================================')
    res = requests.get(link, verify=False)
    soup = BeautifulSoup(res.text, 'html.parser')

    excel_url = ''
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.endswith(('.xls', '.xlsx')):
            excel_url = href

    if not excel_url:
        print('KHÔNG TÌM THẤY FILE EXCEL TRONG BÀI BÁO !!!!!!!!!!')
        return

    # Code Classification Kinds of Excel Times Year - Month
    year = title[len(title) - 4::]

    # trích xuất tháng từ title (bài mới nhất nên không dùng pre_month)
    month = get_month_from_title(title)

    print(year, month)
    create_bucket_if_not_exists('bronze')

    object_name = f"newest_economic_report_excel_file/{year}/{month}/"
    excel_file = requests.get(excel_url, verify=False, stream=True)

    file_name = excel_url.split('/')[-1]

    os.makedirs('/opt/airflow/tmp_data', exist_ok=True)

    local_path = f'/opt/airflow/tmp_data/{file_name}'

    with open(local_path, 'wb') as f:
        f.write(excel_file.content)

    print(f'Tạo file {file_name} thành công')

    upload_path = local_path
    upload_name = file_name

    # convert nếu là .xls
    if file_name.lower().endswith('.xls') \
            and not file_name.lower().endswith('.xlsx'):

        print('Bắt đầu convert XLS -> XLSX')
        output_dir = os.path.dirname(local_path)
        upload_path = convert_xls_to_xlsx_file(local_path, output_dir)

        upload_name = os.path.basename(upload_path)
    clear_prefix_in_minio('bronze', 'newest_economic_report_excel_file' )
    # upload MinIO
    load_file_to_Bronze(
        bucket_name='bronze',
        object_name=f"{object_name}{upload_name}",
        local_file_path=upload_path
    )

    # cleanup
    if os.path.exists(local_path):
        os.remove(local_path)

    if upload_path != local_path \
            and os.path.exists(upload_path):
        os.remove(upload_path)

    print('Xóa file tạm thành công')


craw_and_load_latest_report_economic_excel_file_to_bronze()