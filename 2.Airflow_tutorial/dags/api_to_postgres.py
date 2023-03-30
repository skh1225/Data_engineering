from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests
from sqlalchemy import create_engine

default_args = {
    'owner' : 'sgh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

def get_token(ti):
    REFRESH_TOKEN=""
    Authorization=""
    endpoint = "https://accounts.spotify.com/api/token"
    headers={"Content-Type": "application/x-www-form-urlencoded","Authorization": Authorization}
    data={"grant_type":"refresh_token","refresh_token":REFRESH_TOKEN}

    r = requests.post(endpoint, headers=headers, data=data)
    
    token_info = r.json()
    print(token_info['access_token'])
    ti.xcom_push(key='access_token', value=token_info['access_token'])
    ti.xcom_push(key='token_type', value=token_info['token_type'])

def get_data(access_token,token_type,ts):
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"{token_type} {access_token}"
    }
    from_time = datetime.strptime(ts[:19],'%Y-%m-%dT%H:%M:%S')
    from_time_unix_timestamp = int(from_time.timestamp()) * 1000

    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={from_time_unix_timestamp}&limit=50", headers=headers)
    
    return r.json()

def check_if_valid_data(df: pd.DataFrame,ts) -> bool:
    #Check if dataframe is empty
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is volidated")
    
    if df.isnull().values.any():
        raise Exception("Null valued found")       
    return True

def load_to_psql(ti,ts):
    access_token = ti.xcom_pull(task_ids='get_token', key='access_token')
    token_type = ti.xcom_pull(task_ids='get_token', key='token_type')
    data = get_data(access_token,token_type,ts)

    to_time = datetime.strptime(ts[:19],'%Y-%m-%dT%H:%M:%S') + timedelta(hours=1)
    song_name=[]
    artist_name=[]
    played_at=[]
    
    for song in data["items"]:
        if datetime.strptime(song["played_at"],'%Y-%m-%dT%H:%M:%S.%fZ') <= to_time:
          song_name.append(song["track"]["name"])
          artist_name.append(song["track"]["album"]["artists"][0]["name"])
          played_at.append(song["played_at"])
    
    song_dict = {
        "song_name":song_name,
        "artist_name":artist_name,
        "played_at":played_at
    }

    song_df = pd.DataFrame(song_dict)
    if check_if_valid_data(song_df,ts):
        print("Data valid, proceed to Load stage")
        conn_string = 'postgresql://airflow:airflow@postgres:5432'
        db = create_engine(conn_string)
        conn = db.connect()
        song_df.to_sql('my_played_tracks', con=conn, schema='test', if_exists='append', index=False)
        conn.close()



with DAG(
    dag_id='played_tracks_v2.1',
    start_date=datetime(2023,3,28,0),
    schedule_interval='0 * * * *', # cron expression 'M H d m week'
    default_args=default_args
) as dag:
    task1 = PythonOperator(
        task_id="get_token",
        python_callable=get_token
    )

    task2 = PostgresOperator(
        task_id="create_table",
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists test.my_played_tracks (
            song_name VARCHAR(200) ,
            artist_name VARCHAR(200),
            played_at VARCHAR(200) Primary key
        )
        """
    )
    task3 = PythonOperator(
        task_id="load_to_psql",
        python_callable=load_to_psql
    )
    

    task1 >> task2 >> task3