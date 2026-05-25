
from minio import Minio
import pandas as pd
import io


client = Minio(
    'minio:9000',
    access_key = 'minioadmin',
    secret_key= 'minioadmin',
    secure= False
)

def check_exist_bucket(bucket_name):
    try:
        return client.bucket_exist(bucket_name)
    except Exception as e:
        print(f'An Error Occured When Check Exist Bucket: {bucket_name}', e)

def create_bucket(bucket_name):
    try:
        client.make_bucket(bucket_name)
    except Exception as e:
        print(f'An Error When Create Bucket: {bucket_name}!!!!!!!!!!!', e)

def get_excel_file(bucket_name, file_path):

    try:
        obj = client.get_object(
            bucket_name,
            file_path
        )

        data = obj.read()

        return  pd.ExcelFile(io.BytesIO(data))
        
    except Exception as e:
        print(f"AN ERROR OCCURED WHEN READ SHEET FILE PAHT: {file_path} ", e)

def load_file(bucket_name, object_name, local_file_path):
    try:
        if check_exist_bucket(bucket_name=bucket_name):
            client.fput_object(
                bucket_name,
                object_name,
                local_file_path
            )
        else:
            create_bucket(bucket_name= bucket_name)
            client.fput_object(
                bucket_name,
                object_name,
                local_file_path
            )
    except Exception as e:
        print(f"An Error Occured When Load {local_file_path} to Bucket: {bucket_name} ", e)

    
def get_list_files(bucket_name, prefix):
    try:
        print(f'Dang lay danh sach duong dan file excel trong bucket = {bucket_name}, prefix = {prefix}')
        objects = client.list_objects(
            bucket_name,
            prefix, 
            recursive= True
        )
        print('GET FILE EXCEL PATHS SUCCESSFULLY !!!!')
        paths_list = []
        for obj in objects:
            if obj.is_dir:
                continue
            paths_list.append(obj.object_name)

        return paths_list

    except Exception as e:
        print('AN ERROR OCCURED WHEN GET FILE EXCEL PATHS LIST: ', e)