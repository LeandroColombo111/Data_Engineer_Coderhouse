from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from modules.extraction import extract_api_data, extract_db_data
from modules.transformation import transform_data
from modules.loading import load_data_to_dwh
from modules.alerting import send_alert

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('etl_pipeline', default_args=default_args, schedule_interval=timedelta(days=1))

extract_api = PythonOperator(task_id='extract_api', python_callable=extract_api_data, dag=dag)
extract_db = PythonOperator(task_id='extract_db', python_callable=extract_db_data, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=lambda: transform_data(extract_api.output, extract_db.output), dag=dag)
load = PythonOperator(task_id='load', python_callable=load_data_to_dwh, dag=dag)
alert = PythonOperator(task_id='alert', python_callable=send_alert, dag=dag)

extract_api >> extract_db >> transform >> load >> alert
