from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from snowflake.connector.pandas_tools import write_pandas

from queries import create_db_objects_query, columns
from functions import load_into_raw_table



default_args = {
    'owner': 'coder2j',
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}


with DAG('snowflake', 
            schedule_interval='@daily',
            dagrun_timeout=timedelta(minutes=60),
            start_date=datetime(2022, 8, 4), default_args=default_args) as dag:

    create_tables = SnowflakeOperator(
        dag=dag,
        task_id='create_db_objects',
        snowflake_conn_id='snow',
        sql=f"{create_db_objects_query}"
    ) 

    load_into_raw_table_task = PythonOperator(
    dag=dag,
    task_id='insert_pd_to_snowflake',
    python_callable=load_into_raw_table
    )

    insert_into_stage_table = SnowflakeOperator(
        dag=dag,
        task_id='insert_into_stage_table',
        snowflake_conn_id='snow',
        sql= f'insert into stage_table {columns} from raw_stream;'
    )


    insert_into_master_table = SnowflakeOperator(
        dag=dag,
        task_id='insert_into_master_table',
        snowflake_conn_id='snow',
        sql=f'insert into master_table {columns} from stage_stream;'
    )

    
    create_tables >> load_into_raw_table_task >> insert_into_stage_table >> insert_into_master_table