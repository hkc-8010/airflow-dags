from airflow.models.dag import DAG
from datetime import timedelta
from pendulum import datetime,timezone

local_tz = timezone("Pacific/Auckland")

class DEDag(DAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        common_dag_args = {
            'email_on_failure': True,
            'email_on_retry': False,
            'execution_timeout': timedelta(hours=2),
            'retries': 1,
            'start_date': datetime(2024, 7, 15, tz=local_tz),
            'retry_delay': timedelta(seconds=5),
            #'owner': 'not_defined',
            # 'email': ['BNZ_Airflow_Support@bnz.co.nz'],
        }
        common_dag_args.update(kwargs.get('default_args', {}))
        self.default_args = common_dag_args