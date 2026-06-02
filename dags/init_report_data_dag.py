from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.append('/opt/airflow/tasks')

from datetime import datetime


with DAG (
    dag_id = 'Initiation_Report_Data_Dag',
    start_date = datetime(2025, 1, 1),
    
)as dag:
    # task_1 = BashOperator(
    #     task_id = 'crawl_and_load_to_bronze_layer',
    #     bash_command = 'docker exec python_container python bronze/crawl_and_load_report_excel_files_to_bronze.py'
    # )
    # task_2 = BashOperator(
    #     task_id = 'ddl_silver_layer',
    #     bash_command = 'docker exec spark-master /opt/spark/bin/spark-submit silver/ddl_silver.py'
    # )
    task_3 = BashOperator(
        task_id = 'transform_and_load_data_to_silver',
        bash_command = "docker exec spark-master /opt/spark/bin/spark-submit silver/Extract_Data_From_Excel_Reports.py" 
    )
    
# task_1 >> task_2 >> task_3

# task_2 >> task_3
