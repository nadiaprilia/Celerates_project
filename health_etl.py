from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'health_etl',
    default_args=default_args,
    description='ETL dari OLTP ke OLAP',
    schedule_interval='@daily',
)


def extract_data():
    # Simulasi ekstraksi data dari OLTP
    print("Extracting data from OLTP...")
    # Code for extracting data from OLTP
    # Replace with actual extraction logic

def load_data():
    # Simulasi pemuatan data ke OLAP
    print("Loading data into OLAP...")
    # Code for loading data into OLAP
    # Replace with actual loading logic

def end_task_function():
    print("End task executed")

def do_nothing():
    # Fungsi kosong yang tidak melakukan apa-apa
    pass

start_task = PythonOperator(
    task_id='start_task',
    python_callable=do_nothing,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    dag=dag,
)

spark_task = SparkSubmitOperator(
    task_id='spark_task',
    application='/home/nadiapril239/airflow/dags/spark_script/health_spark_script.py',
    conn_id='spark_default',
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=do_nothing,
    dag=dag,
)

start_task >> extract_task >> spark_task >> load_task >> end_task