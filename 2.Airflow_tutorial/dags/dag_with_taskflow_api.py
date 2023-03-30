from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner' : 'sgh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}


@dag (
    dag_id='dag_with_taskflow_api_v02',
    start_date=datetime(2023,3,26,0),
    schedule_interval='@daily',
    default_args=default_args)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {"first_name":"Gyeonhwan",
        "last_name":"Seo"}

    @task()
    def get_age():
        return 20

    @task()
    def greet(name,age):
        print(f"Hello world My name is {name['first_name']} {name['last_name']},"
        f"and I an {age} years old!")

    name = get_name()
    age = get_age()
    greet(name=name,age=age)

greet_dag = hello_world_etl()