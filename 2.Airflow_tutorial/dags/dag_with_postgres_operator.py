from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner' : 'sgh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_postgres_operator_v07',
    start_date=datetime(2023,3,26,0),
    schedule_interval='0 0 * * *', # cron expression 'M H D m week'
    default_args=default_args
) as dag:
    task2 = PostgresOperator(
        task_id='create_postgres_schema',
        postgres_conn_id='postgres_localhost',
        sql="""
            create schema if not exists test
        """
    )
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists test.dag_runs (
                dt date,
                dag_id varchar(100),
                primary key (dt,dag_id)
            )
        """
    )
    task0 = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into test.dag_runs (dt,dag_id) values 
            ('{{ ds }}','{{ dag.dag_id }}')
        """
    )
    task2 >> task1 >> task0