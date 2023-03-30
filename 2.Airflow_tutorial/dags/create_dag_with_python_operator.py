from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'sgh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

def greet(ti):
    first_name=ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name=ti.xcom_pull(task_ids='get_name', key='last_name')
    age=ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello world My name is {first_name} {last_name},"
    f"and I an {age} years old!")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Gyeonghwan')
    ti.xcom_push(key='last_name', value='Seo')

def get_age(ti):
    ti.xcom_push(key='age', value=20)

with DAG(
    dag_id='our_python_operator_v03',
    description='This is our first dag using python operator',
    start_date=datetime(2023,3,26,0),
    schedule_interval='@daily',
    default_args=default_args
) as dag:
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet
        #op_kargs={'name':'GyeonhwanSeo', 'age':20}
    )

    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3=PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2,task3] >> task1