from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args={
'start_date':datetime(2021,4,20)}


def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


#initialization of DAG
with DAG('Paralleltasks_test',schedule_interval=None,default_args=default_args,catchup=False) as dag:

     #python operator task
     test_operator = PythonOperator(
     task_id='python_op_test',
     python_callable=print_context,
     dag=dag)
     
     #2nd task to test parallelism 
     test_bashOperator = BashOperator(
     task_id='shell_op_test',
     bash_command='sleep 30',
     dag=dag)

     [test_operator,test_bashOperator] 
 
