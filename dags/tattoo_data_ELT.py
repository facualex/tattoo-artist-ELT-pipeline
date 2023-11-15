from datetime import datetime
from os import path
from airflow.models.baseoperator import chain
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task 
from include.soda.run_check import run_check
from include.dbt.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

# Documentation files
from docs.dag_doc import (
    doc_md_DAG,
    extract_data_doc
)

AIRFLOW_POSTGRES_CONN_ID = 'tattoos_raw_data'

default_args = {
    'owner': 'airflow',
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    'start_date': datetime(2023, 11, 12),
    'retries': 3, # If a task fails, it will retry 3 times.
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    'catchup': False,
}

@dag(
    dag_id = "tattoo_data_ELT", # ID name for DAG
    tags=["tattoos_dataset", "ELT"],
    schedule = "@daily", # This DAG will run daily
    default_args = default_args, # Same args will be applied throughout the DAG (all tasks)
    doc_md = doc_md_DAG,
) 
def tattoo_data_ELT():
    # execute Python inside a pre-defined environment (virtualenv)
    @task(task_id = "extract_data", doc_md = extract_data_doc)
    def extract():
        import pandas as pd
        from airflow.models import Variable

        sheet_id = Variable.get("sheet_id")

        if not sheet_id:
           raise ValueError(f"The 'sheet_id' variable is not set. Check if you defined the variable on the Airflow dashboard.")

        # Download Sheets data into a Pandas Dataframe
        df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")

        # Convert all column names to lowercase.
        # SODA data quality framework doesn't seem to properly recognize columns that start with an uppercase letter...
        df.columns = [col.lower() for col in df.columns]

        # Convert the loaded pandas dataframe into a CSV file.
        # Save file into the airflow project instance
        df.to_csv('/usr/local/airflow/include/data.csv')
    
    @task(task_id = "load_data_to_pg")
    def load():
        import pandas as pd

        postgres_hook = PostgresHook(postgres_conn_id=AIRFLOW_POSTGRES_CONN_ID)

        data_path = '/usr/local/airflow/include/data.csv'

        if not path.exists(data_path):
            raise ValueError(f"The data file on {data_path} does not exist. Re-run the DAG to obtain the data file or manually run the 'extract_data' task.")

        df = pd.read_csv(data_path)

        # "if_exists" handles what to do if the table/data already exists
        # "chunksize" commits the rows in chunks (great for large amounts of data)
        df.to_sql('tattoo_raw_data',
                  postgres_hook.get_sqlalchemy_engine(),
                  if_exists='replace',
                  chunksize=1000)

    @task(task_id = "load_quality_check")
    def load_data_quality_check():
        import os

        airflow_home_path = os.environ.get('AIRFLOW_HOME')

        if airflow_home_path is None:
            raise ValueError("AIRFLOW_HOME enviroment variable is not set.")

        checks_file_path = os.path.join(airflow_home_path, 'include/soda/checks/load_checks.yml')
        configuration_file_path = os.path.join(airflow_home_path, 'include/soda/configuration.yml')

        run_check(data_source_name="tattoo_raw_data",
                    check_name = "load_quality_check",
                    check_file_path=checks_file_path,
                    yaml_configuration_file_path=configuration_file_path)

    transform = DbtTaskGroup(
        group_id = 'transform',
        project_config = DBT_PROJECT_CONFIG,
        profile_config = DBT_CONFIG,
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            select = ['path:models/transform']
        ),
    )

    @task(task_id = "transform_quality_check")
    def transform_data_quality_check():
        import os

        airflow_home_path = os.environ.get('AIRFLOW_HOME')

        if airflow_home_path is None:
            raise ValueError("AIRFLOW_HOME enviroment variable is not set.")

        checks_file_path = os.path.join(airflow_home_path, 'include/soda/checks/transform/')
        configuration_file_path = os.path.join(airflow_home_path, 'include/soda/configuration.yml')

        run_check(data_source_name="tattoo_raw_data",
                    check_name = "transform_quality_check",
                    check_file_path=checks_file_path,
                    yaml_configuration_file_path=configuration_file_path)

    chain(extract(),
          load(),
          load_data_quality_check(),
          transform,
          transform_data_quality_check()) 

tattoo_data_ELT()
