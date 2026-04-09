from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append('/opt/airflow/tasks')

from crawl_and_load_report_excel_files_to_bronze import craw_and_load_report_economic_excel_files_to_bronze
from datetime import datetime

def hallo(): 
    print('hêllloooo')

with DAG (
    dag_id = 'example_dag',
    start_date = datetime(2025, 1, 1),
    
)as dag:
    task1 = PythonOperator(
        task_id = 'template',
        python_callable=craw_and_load_report_economic_excel_files_to_bronze
    )