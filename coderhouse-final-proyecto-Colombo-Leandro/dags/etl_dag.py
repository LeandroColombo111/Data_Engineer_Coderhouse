from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from modules.extraction import extract_posts
from modules.loading import load_initial_data, load_combined_data
from modules.transformation import transform_and_combine
from modules.alerting import send_email
import pandas as pd
from dotenv import load_dotenv

# Cargar las variables de entorno
load_dotenv()

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG for loading data to Redshift',
    schedule_interval=timedelta(days=1),
)

def extract():
    posts = extract_posts()
    return posts

def load_initial(posts, **kwargs):
    excel_file_path = 'dataframe/dataframe.xls'
    users_df = pd.read_excel(excel_file_path)
    load_initial_data(posts, users_df)

def transform(**kwargs):
    combined_data = transform_and_combine()
    return combined_data

def load_combined(**kwargs):
    combined_data = kwargs['ti'].xcom_pull(task_ids='transform')
    load_combined_data(combined_data)

def send_alert(**kwargs):
    send_email("ETL Process Completed", "The ETL process has completed successfully.")

# Definir las tareas del DAG
t1 = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

t2 = PythonOperator(
    task_id='load_initial',
    python_callable=load_initial,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='load_combined',
    python_callable=load_combined,
    provide_context=True,
    dag=dag,
)

t5 = PythonOperator(
    task_id='send_alert',
    python_callable=send_alert,
    provide_context=True,
    dag=dag,
)

# Definir el flujo de tareas
t1 >> t2 >> t3 >> t4 >> t5
