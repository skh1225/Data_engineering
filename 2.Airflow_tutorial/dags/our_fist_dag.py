from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'sgh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v4',
    description='This is our first dag',
    start_date=datetime(2023,3,26,0),
    schedule_interval='@daily',
    default_args=default_args
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world'
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo this is task2'
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo this is task3'
    )
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    task1 >> [task2,task3]