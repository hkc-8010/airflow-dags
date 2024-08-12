from cosmos import DbtDag, DbtTaskGroup
from cosmos.config import ProjectConfig, ExecutionConfig, RenderConfig, ProfileConfig
from cosmos.profiles.snowflake.user_pass import SnowflakeUserPasswordProfileMapping
from cosmos.profiles.snowflake.user_privatekey import SnowflakePrivateKeyPemProfileMapping
from pendulum import datetime
import os
from cosmos.constants import LoadMode
from airflow.hooks.base import BaseHook
import json
from airflow.models import Variable

class DEDbtTaskGroup(DbtTaskGroup):
    def __init__(self, group_id=None, operator_args = None, select=[], data_product_type="PEOPLE", *args, **kwargs):
        CONNECTION_ID = "bdh_db_snowflake"
        DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
        DBT_PROJECT_PATH = "/usr/local/airflow/dags/repo/dbt_projects/dbt_main"
        

        connection = BaseHook.get_connection(CONNECTION_ID)
        try:
            database_name = json.loads(connection.extra)["extra__snowflake__database"]
        except:
            raise Exception(json.loads(connection.extra))

        if "ASTRONOMER_ENVIRONMENT" not in os.environ or os.environ["ASTRONOMER_ENVIRONMENT"] != "local":
            target_name = "dev"
            profile = SnowflakeUserPasswordProfileMapping(conn_id=CONNECTION_ID,
                                                          profile_args={ "schema" : "staging", "threads" : 32 })
        else:
            target_name = "local"
            profile = SnowflakePrivateKeyPemProfileMapping(conn_id=CONNECTION_ID)

        profile_config = ProfileConfig(
            profile_name="default",
            target_name=target_name,
            profile_mapping=profile
        )

        execution_config = ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        )

        if not group_id:
            group_id = "transform_data"

        default_operator_args = {
            "append_env": True,
            #TODO : "vars": '{"processed_date": "{{ next_execution_date.in_timezone("Pacific/Auckland").to_date_string() }}" , "raw_database_name": "' + raw_database_name + '"}'
        }

        final_operator_args = default_operator_args
        if operator_args:
            final_operator_args = default_operator_args
            final_operator_args.update(operator_args)

        raw_database_name = Variable.get("src_db" , default_var = None)  
        
        Project_Config = ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            dbt_vars={
                "raw_database_name": raw_database_name,
            },
        )

        super().__init__(
            group_id=group_id,
            project_config=Project_Config,
            profile_config=profile_config,
            execution_config=execution_config,
            operator_args=final_operator_args,
            render_config   =RenderConfig(select=select, load_method=LoadMode.DBT_LS),
            *args, **kwargs)

        self.select = select
        self.data_product_type = data_product_type
