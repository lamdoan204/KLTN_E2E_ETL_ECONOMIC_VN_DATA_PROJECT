import requests, os
from bs4 import BeautifulSoup
import unicodedata
import re
from minio import Minio

from reused_func import clean_text


def get_time_of_next_report(url: str):
    try:
        next
    except Exception as e:
        print(f'HAVE AN ERROR WHEN GET NEXT TIME OF REPORT !!!!!!!!!! \n {e}')

    next


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
        

def load_file_to_Bronze(bucket_name, object_name, excel_file, length):
    try:
        if length is None:
            # fallback (ít chuẩn hơn)
            data = excel_file.content
            client.put_object(
                bucket_name,
                object_name,
                data=io.BytesIO(data),
                length=len(data)
            )
        else:
            client.put_object(
                bucket_name,
                object_name,
                data=excel_file.raw,
                length=int(length)
            )
    except Exception as e:
        print(f'HAVE AN ERROR WHEN LOAD FILE TO {bucket_name} !!!!!!!!!!')
        print(e)
    next

def craw_and_load_report_economic_excel_files_to_bronze():
    # url link dẫn đến trang báo cáo kinh tế Việt Nam
    base_url= 'https://www.nso.gov.vn/bao-cao-tinh-hinh-kinh-te-xa-hoi-hang-thang/'
    
    # phân trang
    page = 1
    flag = True

    titles = [] # lưu các tên của bài viết để phân loại
    links = [] # lưu đường dẫn của các bài viêt 
    
    next_report_time = get_time_of_next_report(base_url)
    # Lấy tên bài báo + link dẫn tới bài báo
    print('Lấy tên bài báo + link dẫn tới bài báo................................')

    while flag: 
        if page == 1: 
            url = base_url
        else:
            url = base_url+ f'?paged={page}'

        print(f'Crawling Page: {page} ...........................')

        res = requests.get(url, verify=False)
        soup = BeautifulSoup(res.text, "html.parser")
        container = soup.find('div', class_= 'archive-container')
        the_a = container.find_all('a', class_= None)
        the_h3 = container.find_all('h3', class_=None)
        
        # Lấy title bài báo cáo
        this_page_titles = []
        for x in the_h3:
            t = x.get_text(strip= True)
            if '2024' in t:
                flag = False
                break
            this_page_titles.append(t)
            
        # lấy link bài báo cáo 
        this_page_links = []
        for x in the_a:
            t = x['href']
            if '2024' in t: break
            this_page_links.append(t)

        titles += this_page_titles
        links += this_page_links
        
        page += 1    
    
    # vào bài báo tải file excel về 
    print('vào bài báo tải file excel về ===========================================')
    pre_month = None

    for i in range(len(titles)):
        title =  str.lower(titles[i])
        link = links[i]

        if 'baocaotinhhinhkinhtexahoi' not in clean_text(title) and isinstance(title, str):
            continue
        print(title)
        res = requests.get(link, verify= False)
        soup = BeautifulSoup(res.text, 'html.parser')

        excel_url = '' 
        for a in soup.find_all('a', href= True):
            href = a['href']
            if href.endswith(('.xls', '.xlsx')):
                excel_url =  href

    
        # Code Classification Kinds of Excel Times Year - Month
        year = title[len(title) -4 ::]

        # trích xuất tháng từ title
        month = None
        if pre_month != None: 
            if pre_month != 1: 
                month = pre_month - 1
            else: month = 12
        else:
            temp_title = title[::len(title) - 3]
            if any(e in temp_title for e in ['1', 'một']):
                month = 1
            if any(e in temp_title for e in ['2', 'hai']):
                month = 2
            if any(e in temp_title for e in ['3', 'ba', 'quý i']):
                month = 3
            if any(e in temp_title for e in ['4', 'bốn', 'tư']):
                month = 4
            if any(e in temp_title for e in ['5', 'năm']):
                month = 5
            if any(e in temp_title for e in ['6', 'sáu', 'quý ii']):
                month = 6
            if any(e in temp_title for e in ['7', 'bảy']): 
                month = 7
            if any(e in temp_title for e in ['8', 'tám']):
                month = 8
            if any(e in temp_title for e in ['9', 'chín', 'quý iii']):
                month = 9
            if any(e in temp_title for e in ['10', 'mười']):
                month = 10
            if any(e in temp_title for e in ['11', 'mười một']):
                month = 11
            if any(e in temp_title for e in ['12', 'mười hai', 'quý iv']):
                month = 12
        
        pre_month = month

        create_bucket_if_not_exists('bronze')

        object_name = f"economic_report_excel_files/{year}/{month}"
        excel_file = requests.get(excel_url, verify= False, stream= True)
        length = excel_file.headers.get("content-length")

        # Code push data to MinIO Bronze

        load_file_to_Bronze(bucket_name='bronze', object_name= object_name, excel_file= excel_file, length= length)
        
        

        
