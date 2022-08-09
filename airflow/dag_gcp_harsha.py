from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from datetime import datetime, timedelta

"""
Connection Id: databricks_jobs_demo_csc
Connection Type: Databricks
Host: https://adb-5108423936237030.10.azuredatabricks.net/
Login token
Password: dapi1c8b47ab0c7831ad3892f8a9ebd589de

skipped: Extra: {"token": "dapi1c8b47ab0c7831ad3892f8a9ebd589de"}

Link: https://medium.com/@paulomiguelbarbosa/integrating-airflow-with-databricks-simple-use-case-616869e1e4e
{"job_id":457269675075675, "notebook_params": {"name": "csc airflow"}}
"""

"""
Broken DAG: [/home/airflow/gcs/dags/databricks_jobs_demo_csc.py] Traceback (most recent call last):
  File "<frozen importlib._bootstrap>", line 219, in _call_with_frames_removed
  File "/home/airflow/gcs/dags/databricks_jobs_demo_csc.py", line 3, in <module>
    from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
ModuleNotFoundError: No module named 'airflow.providers.databricks'
pip install apache-airflow-providers-databricks
"""

"""
SUCCESS SCENARIO.
- It will run the {"job_id":457269675075675, "notebook_params": {"name": "csc airflow"}}, that is already created in databricks.
- Basically, it will trigger what that job is supposed to do (that is, run a notebook)
"""

# Define params for Run Now Operator
notebook_params = {"name": "harsha airflow"}

with DAG(
    "databricks_job_dag",
    start_date=datetime(2022, 8, 8),
    schedule_interval="@once",
    catchup=False,
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    t0 = DummyOperator(
        task_id='start_task'
    )

    opr_run_now = DatabricksRunNowOperator(
        databricks_conn_id="databricks_jobs_demo_harsha",
        job_id=261103515012822,
        notebook_params=notebook_params,
    )

    t0 >> opr_run_now
