from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'people',
    'depends_on_past': False,
    'retries': 1,
    'start_date':datetime(2022,7,1),
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    'dp_people_extract',
    default_args=default_args,
    description='call sf stored procedure to extract data',
    schedule_interval=None,
    tags = ['people'],
    catchup=False)


data_product_extract = SnowflakeOperator(
    task_id='call_extract_sp',
    snowflake_conn_id='bdh_db_snowflake',
    sql = 'sql/snowflake/dp_people_generate_extract_sp.sql',
    autocommit=False,
    dag=dag)

data_product_extract