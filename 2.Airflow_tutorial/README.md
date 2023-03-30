reference : https://youtu.be/K9AnJ9_ZAXE

```
├── dags
│   ├── our_fist_dag.py
│   ├── dag_with_taskflow_api.py
│   ├── dag_with_postgres_operator.py
│   ├── create_dag_with_python_operator.py 
│   ├── dag_with_catchup_and_backfill.py   
│   ├── dag_with_minio_s3.py
│   ├── api_to_postgres.py 
│   └── postgres_to_redshift.py
├── docker-compose.yaml
├── dockerfile
└── requirements.txt
```
`api_to_postgres.py`
- played_tracks_v2.1(DAG) : 1시간 간격으로 아래의 task를 순서대로 실행
  - get_token(task1) : Spotify API 에 Refresh token을 이용해 Access token을 생성
  - create_table(task2) : postgres 에 table 생성
  - load_to_psql(task3) : Spotify API로 Recently_played_track 을 받아 postgres에 적재
=> Spotify에서는 사용자가 들은 최근 50곡만 저장해서 1시간 간격으로 API를 호출하기로 결정

`postgres_to_redshift.py`
- to_redshift_v04(DAG) : 하루 간격으로 아래의 taks를 순서대로 실행
  - postgres_to_s3(task1) : Postgres 에 저장된 데이터를 s3 에 csv 형태로 저장
  - create_table(task2) : redshift 에 table 생성
  - transfer_data_to_redshift(task3) : S3에 저장된 csv를 Redshift로 bulk insert(COPY)
=> S3 대신 MINIO로 대체, Redshift와 S3 region이 달라 task3에서 에러  
