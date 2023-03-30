import csv
import logging
import requests
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from tempfile import NamedTemporaryFile

default_args = {
    'owner' : 'sgh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

def postgres_to_s3(ds, next_ds):
    # step 1: query data from postgresql db and save into text file
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from test.my_played_tracks where LEFT(played_at,10) >= %s and LEFT(played_at,10) < %s",
                   (ds, next_ds))
    with NamedTemporaryFile(mode='w', suffix=f"{ds}") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
    # step 2: upload text file into S3
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=f.name,
            key=f"tracks/{ds}.csv",
            bucket_name="airflow",
            replace=True
        )
        logging.info("tracks file %s has been pushed to S3!", f.name)




with DAG(
    dag_id='to_redshift_v04',
    start_date=datetime(2023,3,28,0),
    schedule_interval='0 0 * * *', # cron expression 'M H d m week'
    default_args=default_args
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )

    task2 = RedshiftSQLOperator(
        task_id='create_table',
        sql="""
            create table if not exists my_played_tracks (
            song_name VARCHAR(200) ,
            artist_name VARCHAR(200),
            played_at VARCHAR(200) Primary key
            );
        """,
        redshift_conn_id="redshift_conn"
    )

    task3 = S3ToRedshiftOperator(
    task_id="transfer_data_to_redshift",
    redshift_conn_id="redshift_conn",
    s3_bucket="airflow",
    s3_key="tracks/{{ ds }}.csv",
    schema="PUBLIC",
    table="my_played_tracks",
    copy_options=["csv"],
    aws_conn_id="minio_conn"
    )

    task1 >> task2 >> task3
    
