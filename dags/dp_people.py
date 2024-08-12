from dags.libs.dags.de_dag import DEDag
from dags.libs.operators.de_dbt_operators import DEDbtTaskGroup
from pendulum import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import kubernetes.client.models as k8s

# Define the resource requests and limits for CPU and Memory
custom_resource = k8s.V1ResourceRequirements(
    requests={
        "cpu": "0.5",
        "memory": "750Mi"
        },
    limits={
        "cpu": "0.5",
        "memory": "750Mi"
        }      
)

# executor_config to override the pod spec with custom_resource
executor_config = {
    "KubernetesExecutor": {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=custom_resource,
                        )
                ]
            )
        )
    }
}

with DEDag(
    "dp_people",
    description="An Airflow DAG for staging data people from raw source",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs = 1,
    tags = ['people']
) as dag:
    pre_process_dp_people = DEDbtTaskGroup(
        group_id = "pre_process",
        select=["tag:pre_processing"],
    )

    process_dp_people = DEDbtTaskGroup(
        group_id = "main_process",
        select=["tag:main_processing"],
    )

    post_process_dp_people = DEDbtTaskGroup(
        group_id = "post_process",
        select=["tag:post_processing"],
    )

    call_extract_sp = TriggerDagRunOperator(
        task_id = "call_extract_sp",
        trigger_dag_id = "dp_people_extract"

    )
    pre_process_dp_people >> process_dp_people >> post_process_dp_people >> call_extract_sp