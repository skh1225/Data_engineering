from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'sgh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_catchup_backfill_v10',
    start_date=datetime(2023,3,25,1),
    schedule_interval='0 0 * * *', # cron expression 'M H D m week'
    default_args=default_args,
    catchup=True
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command!'
    )

    task1