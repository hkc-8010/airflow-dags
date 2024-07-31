from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 

airflow_include_dir = os.path.join(os.environ['AIRFLOW_HOME'], 'include')

default_args = {
    "owner": "airflow",
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
}
 
def push_config(ti):
    current_datetime = datetime.now()
    ti.xcom_push(key='snapshot_id', value=current_datetime)
    print(f'Pushed {current_datetime} to XCom with key snapshot_id.')

with DAG('main_dag', start_date=datetime(2023, 11, 8, tzinfo=pendulum.timezone("America/New_York")),
         schedule='*/5 * * * *', max_active_runs=1, catchup=False, template_searchpath=airflow_include_dir,
         default_args=default_args, tags=["basic"]) as dag:

    push_config = PythonOperator(
        task_id='push_config',
        python_callable=push_config
    )

    child_dag_trigger = TriggerDagRunOperator(
        task_id="child_dag_trigger",
        trigger_dag_id="child_dag",
        retries=0,
        wait_for_completion=True,
        deferrable=True
    )

    push_config >> child_dag_trigger