from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig # Added ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import LoadMode
from datetime import timedelta

# 1. Define the internal path where your dbt folder is mounted
# In your docker-compose, you mapped ./dbt to /opt/airflow/dbt
DBT_PROJECT_PATH = Path("/opt/airflow/dbt")

# 2. Map the Airflow Connection to dbt
# Cosmos will automatically pull the Account, User, and Password from your 'snowflake_conn'
profile_config = ProfileConfig(
    profile_name="tpch_snowflake_project", # Must match the 'profile' name in dbt_project.yml
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", # The name of the connection you created in Airflow UI
        profile_args={
            "database": "dbt_production",
            "warehouse": "dbt_wh",
            "role": "dbt_role",
            "schema": "tpch_prod",
        },
    ),
)

def dag_failure_callback(context):

    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    error_msg = f"❌ Task Failed! \nDAG: {dag_id} \nTask: {task_id}"
    
    print(error_msg)

with DAG(
    dag_id="dbt_cosmos_snowflake",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 3. Create the TaskGroup that renders dbt models as Airflow tasks
    dbt_run_group = DbtTaskGroup(
        # group_id="dbt_daily_run", # for daily runs
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
        operator_args={
            "install_deps": True,  # This runs 'dbt deps' automatically
            "on_failure_callback": dag_failure_callback,
        },
        render_config=RenderConfig(
            LoadMode.DBT_LS, 
            # select=["tag:daily"], # This is for daily runs in the future
        ),
    )

    dbt_run_group