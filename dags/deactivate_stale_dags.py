from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.utils import timezone
from airflow.utils.file import list_py_file_paths, might_contain_dag


manager = DagFileProcessorManager(
    dag_directory='/usr/local/airflow/dags/repo/dags',
    max_runs=1,
    processor_timeout=30,
    dag_ids=None,
    pickle_dags=False,
)

now = timezone.utcnow()


elapsed_time_since_refresh = (now - manager.last_deactivate_stale_dags_time).total_seconds()

if elapsed_time_since_refresh > manager.parsing_cleanup_interval:
    file_paths = list_py_file_paths(manager.get_dag_directory())

    last_parsed = {
        fp: manager.get_last_finish_time(fp) for fp in manager.file_paths if manager.get_last_finish_time(fp)
    }
    DagFileProcessorManager.deactivate_stale_dags(
        last_parsed=last_parsed,
        dag_directory=self.get_dag_directory(),
        stale_dag_threshold=self.stale_dag_threshold,
    )
    self.last_deactivate_stale_dags_time = timezone.utcnow()