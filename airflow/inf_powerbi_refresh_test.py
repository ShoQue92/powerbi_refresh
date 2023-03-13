from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import os, yaml
from operators.powerbi_refresh_operator import PowerBIRefreshOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

log = logging.getLogger(__name__)
ENV = Variable.get("ENV")
current_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(current_dir, "config.yaml")

with open(configuration_file_path) as yaml_file:
    config = yaml.safe_load(yaml_file)
    
local_args = {
    'retries': 0, 
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id="inf_powerbi_refresh_test",
        description="Testje voor PowerBI refresh via Python VirtualEnv operator",
        start_date=datetime(2023, 3, 7),
        schedule= "00 21 * * *",
        max_active_runs=1,
        default_args={**config['common_args'], **local_args},
        catchup=False,
        tags = ['PowerBI'],
        concurrency=10
)as dag:

    start = DummyOperator(task_id='start', dag=dag)

    with TaskGroup('powerbi_refresh_test') as powerbi_refresh_test:

        powerbi_refresh_test = PowerBIRefreshOperator(
            task_id = 'powerbi_refresh_test',
            ENV = ENV,
            action = 'refresh_dataset_by_names',
            workspace = '<naam van PowerBI workspace>',
            object = '<naam van dataset>'
        )
        
    start >> powerbi_refresh_test
    end = DummyOperator(task_id='end', dag=dag)
    powerbi_refresh_test >> end