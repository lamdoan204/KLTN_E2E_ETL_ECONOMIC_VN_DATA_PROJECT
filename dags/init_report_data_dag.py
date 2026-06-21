from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.date_time import DateTimeSensor
import requests, os
from bs4 import BeautifulSoup
import unicodedata
import re
from datetime import datetime, timezone, timedelta
import pytz


import sys
sys.path.append('/opt/airflow/tasks')

from datetime import datetime

VN_TZ = timezone(timedelta(hours=7))

def get_time_of_next_report(url = 'https://www.nso.gov.vn/bao-cao-tinh-hinh-kinh-te-xa-hoi-hang-thang/'):
    """
    Lấy thời gian (datetime) dự kiến công bố báo cáo kinh tế - xã hội tiếp theo
    từ bài viết đầu tiên (mới nhất) trên trang danh sách báo cáo NSO.

    Trả về:
        datetime (tzinfo = GMT+7) nếu parse thành công.
        None nếu không tìm thấy thông tin hoặc có lỗi xảy ra.
    """
    try:
        res = requests.get(url, verify=False, timeout=15)
        res.raise_for_status()
    except Exception as e:
        print(f'HAVE AN ERROR WHEN GET NEXT TIME OF REPORT (REQUEST FAILED) !!!!!!!!!! \n {e}')
        return None

    try:
        soup = BeautifulSoup(res.text, 'html.parser')
        container = soup.find('div', class_='archive-container')
        if container is None:
            print('HAVE AN ERROR WHEN GET NEXT TIME OF REPORT: KHÔNG TÌM THẤY archive-container !!!!!!!!!!')
            return None

        # Bài viết đầu tiên trong container = báo cáo mới nhất
        next_release_span = container.find('span', class_='archive-next-release')
        if next_release_span is None:
            print('HAVE AN ERROR WHEN GET NEXT TIME OF REPORT: KHÔNG TÌM THẤY span archive-next-release !!!!!!!!!!')
            return None

        # Text dạng: "Lần công bố sắp tới: 03/07/2026"
        raw_text = next_release_span.get_text(strip=True)
        if ':' not in raw_text:
            print(f'HAVE AN ERROR WHEN GET NEXT TIME OF REPORT: FORMAT LẠ -> "{raw_text}" !!!!!!!!!!')
            return None

        date_str = raw_text.split(':', 1)[1].strip()
        next_report_date = datetime.strptime(date_str, '%d/%m/%Y').replace(tzinfo=VN_TZ)
        next_report_date = next_report_date + timedelta(days=1)
        next_report_date_utc = next_report_date.astimezone(pytz.UTC)


        return next_report_date_utc.isoformat()

    except Exception as e:
        print(f'HAVE AN ERROR WHEN GET NEXT TIME OF REPORT (PARSE FAILED) !!!!!!!!!! \n {e}')
        return None


with DAG (
    dag_id = 'Initiation_Report_Data_Dag',
    start_date = datetime(2025, 1, 1),
    
)as dag:
    task_1 = BashOperator(
        task_id = 'crawl_and_load_to_bronze_layer',
        bash_command = 'docker exec python_container python bronze/crawl_and_load_report_excel_files_to_bronze.py'
    )
    task_2 = BashOperator(
        task_id = 'ddl_silver_layer',
        bash_command = 'docker exec spark-master /opt/spark/bin/spark-submit silver/ddl_silver.py'
    )
    task_3 = BashOperator(
        task_id = 'transform_and_load_data_to_silver',
        bash_command = "docker exec spark-master /opt/spark/bin/spark-submit silver/main.py" 
    )
    task_4 = BashOperator(
        task_id = 'ddl_gold_layer',
        bash_command = "docker exec spark-master /opt/spark/bin/spark-submit gold/ddl_gold_layer.py" 
    )
    task_5 = BashOperator(
        task_id = 'load_data_to_gold_layer',
        bash_command = 'docker exec spark-master /opt/spark/bin/spark-submit gold/load_data_to_gold_layer.py' 
    )
    get_time_task = PythonOperator(
        task_id = 'Get_newest_report_time',
        python_callable = get_time_of_next_report
    )
    trigger_dag_newest = TriggerDagRunOperator(
        task_id = "trigger_newest_report_dag",
        trigger_dag_id = 'Newest_Report_Dag',
        conf={'target_time': "{{ ti.xcom_pull(task_ids='Get_newest_report_time') }}"}
    )

task_1 >> get_time_task >> trigger_dag_newest
task_1 >> task_2 >> task_3 >> task_4 >> task_5



with DAG(
    dag_id  = "Newest_Report_Dag",
    start_date  = datetime(2025, 1, 1),
) as dag:
    wait_for_report_time = DateTimeSensor(
        task_id='wait_for_report_time',
        # Lấy thời gian tương lai được truyền từ DAG A sang
        target_time="{{ dag_run.conf['target_time'] }}",
        
        # LƯU Ý CỰC KỲ QUAN TRỌNG: Phải dùng mode='reschedule'
        # Nó giúp giải phóng tài nguyên (Worker slot). Cứ đến giờ nó mới bật dậy kiểm tra,
        # tránh việc chiếm dụng máy chủ Airflow của bạn suốt nhiều ngày liền.
        mode='reschedule', 
        poke_interval=600 # Cứ mỗi 10 phút kiểm tra lại xem tới giờ chưa
    )
    task_6 = BashOperator(
        task_id = 'Craw_and_load_newest_report_to_Bronze',
        bash_command = 'docker exec python_container python bronze/crawl_and_load_newest_report.py'
    )
    task_7 = BashOperator(
        task_id = 'Processing_newest_data_and_load_to_Silver_layer',
        bash_command = 'docker exec spark-master /opt/spark/bin/spark-submit silver/main_2.py'
    )
    get_time_task = PythonOperator(
        task_id = 'Get_newest_report_time',
        python_callable = get_time_of_next_report
    )
    trigger_dag_newest = TriggerDagRunOperator(
        task_id = "trigger_newest_report_dag",
        trigger_dag_id = 'Newest_Report_Dag',
        conf={'target_time': "{{ ti.xcom_pull(task_ids='Get_newest_report_time') }}"}
    )

wait_for_report_time >> task_6 >> task_7 >> get_time_task >> trigger_dag_newest
