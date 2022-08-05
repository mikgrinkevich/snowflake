from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd

# def load_into_raw_table():
#     df = pd.read_csv('/home/nikol/airflow/data/data.csv')
#     df.columns = map(lambda x: str(x).upper(), df.columns)
#     hook = SnowflakeHook(snowflake_conn_id='snow')
#     engine = hook.get_sqlalchemy_engine()
#     try:
#         with engine.connect().execution_options(autocommit=True) as conn:
#             df.to_sql('RAW_TABLE'.lower() , con=conn, if_exists='append', index=False, method=pd_writer)
#     except Exception as e:
#         print(e)
#     finally:
#         engine.dispose()


def load_into_raw_table():
    df = pd.read_csv('/home/nikol/airflow/data/data.csv')
    df.columns = map(lambda x: str(x).upper(), df.columns)
    hook_connection = SnowflakeHook(snowflake_conn_id='snow')
    engine = hook_connection.get_sqlalchemy_engine()
    start = 0
    step = 10000
    try:
        connection = engine.connect().execution_options(autocommit=True)

        for i in range(df.shape[0] // step + 1):
            df.iloc[start:start+step, :].to_sql("raw_table", con=connection, if_exists='append', index=False)
            start += step
    finally:
        connection.close()
        engine.dispose()