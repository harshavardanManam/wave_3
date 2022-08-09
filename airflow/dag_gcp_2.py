from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 
from airflow.operators.dummy import DummyOperator

#Define params for Submit Run Operator
new_cluster = {
    'spark_version': '11.1.x-scala2.12',
    'num_workers': 2,
    'node_type_id': 'Standard_F8',
}

notebook_task = {
    'notebook_path': '/Users/harshavardan.manam@databricks.com/Wave_3/section_3_1',
}

#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('databricks_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_jobs_demo_harsha',
        new_cluster=new_cluster,
        notebook_task=notebook_task
    )
    t0 = DummyOperator(
        task_id='start_task'
    )

    t0 >> opr_submit_run