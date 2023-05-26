from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
import import_new_dataset
import test_unitaire

my_immo_dag = DAG(
    dag_id='immo_dag',
    description='My Airflow MLOps project DAG',
    tags=['MLOps_project_airflow_datascientest'],
    schedule_interval='0 0 1 6,12 *',
    start_date=datetime(2023, 6, 1),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
    }
)

task_1 = PythonOperator(
    task_id='import_new_data',
    python_callable=import_new_dataset.import_new_data_main,
    dag=my_immo_dag
)
