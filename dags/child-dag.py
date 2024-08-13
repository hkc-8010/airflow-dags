from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 8),
    'retries': 3,
    'retry_delay': timedelta(seconds=60)
}

def pull_config(ti):
    value = ti.xcom_pull(
        dag_id='main_dag', task_ids='push_config', key='snapshot_id', include_prior_dates=True)
    print(value)

with DAG('child_dag', default_args=default_args, schedule=None, catchup=False, tags=["reporting"]) as dag:

    pull_config = PythonOperator(
        task_id='pull_config',
        python_callable=pull_config
    )

    pull_config