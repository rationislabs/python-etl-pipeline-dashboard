from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import etl
dag = DAG(
'traffic_etl',
start_date=datetime(2025, 10, 16),
schedule_interval='@daily',
catchup=False
)
etl_task = PythonOperator(
task_id='run_etl',
python_callable=etl.run_etl,
op_kwargs={'use_spark': True},  # Enable Spark in cloud
dag=dag
)