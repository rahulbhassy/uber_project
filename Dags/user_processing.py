from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    'user_processing',
    start_date=datetime(2025, 2, 12),
    schedule_interval='@daily',
    catchup=False
) as dag:
    # Your tasks go here
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id = 'postgres',
        sql = '''
                CREATE TABLE IF NOT EXISTS users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
                );
        '''
    )