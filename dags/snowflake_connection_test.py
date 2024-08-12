from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago


# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'infa',
    'depends_on_past': False,
    'retries': 1,
    'start_date':datetime(2022,7,1),
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    'snowflake_connectivity_test',
    default_args=default_args,
    description='Snowflake test bdm sql',
    schedule_interval=None,
    tags = ['snowflake', 'testcase'],
    catchup=False)


SOJ = DummyOperator(
    task_id='SOJ',
    dag=dag)

EOJ = DummyOperator(
    task_id='EOJ',
    dag=dag)


connection_test = SnowflakeOperator(
    task_id='connection_test',
    snowflake_conn_id='bdh_db_snowflake',
    sql = 'sql/snowflake/snowflake_connection_test.sql',
    autocommit=False,
    dag=dag)

SOJ >> connection_test >> EOJ