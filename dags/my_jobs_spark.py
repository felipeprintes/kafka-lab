from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "felipeprintes",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "retries": 0
}

## Nomeando e defindo quando a DAG será executada
with DAG(
    "my-dag",
    schedule_interval = timedelta(minutes=10),
    catchup = False,
    default_args = default_args
) as dag:
    ## Defindo as tarefas que serão executadas
    t1 = BashOperator(
        task_id = "my-spark-job",
        bash_command = """
            cd ~
            python dags/etl_jobs/ref_data_s3.py
        """
    )

    t2 = BashOperator(
        task_id = "sucess-task",
        bash_command = """
            cd ~
            python dags/etl_jobs/sucess.py
        """
    )

    t1 >> t2