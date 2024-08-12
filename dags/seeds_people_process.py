from dags.repo.dags.libs.dags.de_dag import DEDag
from dags.repo.dags.libs.operators.de_dbt_operators import DEDbtTaskGroup
from pendulum import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DEDag(
    "seeds_people",
    description="An Airflow DAG for loading seeds data",
    schedule_interval="@once",
    catchup=False,
    max_active_runs = 1,
    tags = ['people']
) as dag:
    process_seeds_people = DEDbtTaskGroup(
        group_id = "seeds_process",
        operator_args={"full_refresh": True},
        select = ["path:seeds"],
    )

    process_dp_people = TriggerDagRunOperator(
        task_id = "process_dp_people",
        trigger_dag_id = "dp_people",
        deferrable = True, 
    )
    process_seeds_people >> process_dp_people