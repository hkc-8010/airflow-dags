from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pprint

CONNECTION_ID = "bdh_db_snowflake"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_PATH = "/usr/local/airflow/dags/repo/dbt_projects/dbt_main"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'retries': 0,
}

dag = DAG(
    dag_id="demo",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
)

def print_env_var():
    source_database = Variable.get("src_db" , default_var = None)
    print("current source database: " , source_database)
    key = '"AIRFLOW__COSMOS__CACHE_DIRAIRFLOW__COSMOS__CACHE_DIR"'
    value = os.environ["ASTRONOMER_ENVIRONMENT"] 
    print("the environment value is : " , value)

    env_var = os.environ 
    # Print the list of user's 
    print("User's Environment variable:") 
    pprint.pprint(dict(env_var), width = 1) 
    
print_context = PythonOperator(
    task_id="print_env",
    python_callable=print_env_var,
    dag=dag,
)
    
   