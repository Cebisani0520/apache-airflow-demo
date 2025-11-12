import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator
from sqlalchemy import create_engine

DEFAULT_ARGS = {
'owner': 'airflow',
'depends_on_past': False,
'retries': 1,
'retry_delay': timedelta(minutes=1),
}


with DAG(
dag_id='etl_spark_postgres_demo',
default_args=DEFAULT_ARGS,
start_date=datetime(2025, 1, 1),
schedule_interval=None,
catchup=False,
tags=['example','etl','spark']
) as dag:


spark_submit = SparkSubmitOperator(
task_id='run_spark_transform',
application='/opt/spark-apps/transform.py',
conn_id='spark_default',
total_executor_cores=1,
name='etl_transform',
application_args=[],
dag=dag
)


def load_parquet_to_postgres(**context):
# read parquet file (pandas can read parquet if pyarrow installed in airflow image)
path = '/opt/spark-apps/output/transformed.parquet'
# using pandas for simplicity; in prod use COPY or SQLAlchemy bulk upsert
import pyarrow.parquet as pq

table = pq.read_table(path)
df = table.to_pandas()
# quick column rename to match DB
engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow_db')
df.to_sql('customers', engine, if_exists='append', index=False)


load_to_db = PythonOperator(
task_id='load_to_postgres',
python_callable=load_parquet_to_postgres,
)


spark_submit >> load_to_db
