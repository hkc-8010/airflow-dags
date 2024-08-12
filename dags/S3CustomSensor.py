# How to Run
# Use dag with config option and pass below details - Here bucket_key is location of the file the dag will sense from bucket mentioned in bucket_name
# {"bucket_key": "sample/t.csv", "wildcard_match": "True", "bucket_name": "bdh-gwfs-dev-01"}

from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
#from bdh.sensors.FCH_S3KeySensor import S3KeySensor
from bdh.sensors.FCH_S3KeySensor import S3KeySizeSensor
from bdh.operators.processing_date_operator import ProcessingDateOperator

# imports for custom Hook
from airflow.hooks.base_hook import BaseHook
import os
import subprocess
import re
import argparse
import shutil
import sys
import logging
import glob
import urllib.parse
from urllib.parse import urljoin


# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
'owner': 'infa',
'depends_on_past': False,
'email': ['satish_doddi@bnz.co.nz'],
'email_on_failure': True,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=1),
#'start_date': datetime.now() - timedelta(seconds=10),
'start_date':datetime(2021,4,29)
# 'queue': 'bash_queue',
# 'pool': 'backfill',
# 'priority_weight': 10,
# 'end_date': datetime(2016, 1, 1),
# 'wait_for_downstream': False,
# 'dag': dag,
# 'adhoc':False,
# 'sla': timedelta(hours=2),
# 'execution_timeout': timedelta(seconds=300),
# 'on_failure_callback': some_function,
# 'on_success_callback': some_other_function,
# 'on_retry_callback': another_function,
# 'trigger_rule': u'all_success'
}


dag = DAG(
'S3Key_CustomSensor',
default_args=default_args,
description='DAG to test S3 Key sensor',
schedule_interval="*/30 * * * *",
tags = ['fch','sensor', 'S3KeySensor', 'S3CustomSensor'],
catchup=False)



# Printing start date and time of the DAG

print_date_bdmlog = BashOperator(
task_id='print_date_log_paramfl',
default_args=default_args,
bash_command='date',
dag=dag)


dyn_bucket_key = S3KeySensor(
task_id= "dyn_bucket_key_task",
default_args=default_args,
#bucket_key='s3://bdh-nonprod-informatica-binaries/2021-08-02/09/17/S3keySensorTestfile.txt',
#bucket_key='{{ macros.fch_date_macros.fch_proc_dates(ti, \"previous_processing_date\") }}/*/*/*.txt',
#bucket_key='AnalyticsProcessID/File/AR/FULL/{{ macros.fch_date_macros.fch_proc_dates(ti, \"processing_date\") }}/{{ macros.fch_date_macros.fch_proc_dates(ti, \"processing_date\") }}_*/*.txt',
bucket_key='{{ dag_run.conf["bucket_key"] }}',
#bucket_key=str(glob.glob(os.path.join(datetime.today().strftime('%Y-%m-%d'),'*/*/*.txt'))),
#wildcard_match = True,
wildcard_match ='{{ dag_run.conf["wildcard_match"] }}',
bucket_name= '{{ dag_run.conf["bucket_name"] }}',
soft_fail = False,
poke_interval = 60,
timeout = 300,
mode = "reschedule",
aws_conn_id= "dap_bdh_aws"
#latest_mod_key = True
)

bashop_test = BashOperator(
task_id='bashop_test_task',
default_args=default_args,
bash_command='date',
dag=dag)





print_date_bdmlog >> dyn_bucket_key >> bashop_test


