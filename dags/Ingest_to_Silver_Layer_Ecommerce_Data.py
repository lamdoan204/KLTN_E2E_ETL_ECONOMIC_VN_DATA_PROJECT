from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

def hallo(): 
    print('hêllloooo')

with DAG (
    dag_id = 'example_dag',
    start_date = datetime(2025, 1, 1),
    
)as dag:
    task1 = PythonOperator(
        task_id = 'say_hello',
        python_callable=hallo
    )