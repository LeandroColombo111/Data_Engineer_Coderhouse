from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

# Asegurarse de que el directorio de trabajo está en el path de Python
sys.path.insert(0, '/app')

from main import main  # Importar la función main desde tu script

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'daily_data_pipeline',
    default_args=default_args,
    description='A simple daily DAG to run main.py',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

run_main_script = PythonOperator(
    task_id='run_main_script',
    python_callable=main,
    dag=dag,
)

run_main_script
