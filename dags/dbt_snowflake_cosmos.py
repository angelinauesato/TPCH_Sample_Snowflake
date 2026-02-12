from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig # Added ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import LoadMode
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

with DAG(
    dag_id="dbt_cosmos_snowflake",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 3. Create the TaskGroup that renders dbt models as Airflow tasks
    dbt_run_group = DbtTaskGroup(
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
        operator_args={
            "install_deps": True, # This runs 'dbt deps' automatically
        },
        render_config=RenderConfig(
            LoadMode.DBT_LS, 
            #select=["path:models"], # By removing the select, it will read models, snapshot and macros
        ),
    )

    dbt_run_group