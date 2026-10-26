# buat koneksiin ke postgres, ambil data dari postgres
import datetime as dt
from datetime import timedelta
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator



def queryPostgresql():
    conn=db.connect(dbname = 'milestone_3',
                    host = 'postgres',
                    port = 5432,
                    user = 'airflow',
                    password = 'airflow')
    df=pd.read_sql("select * from public.table_m3",conn)
    df.to_csv('/opt/airflow/dags/data_raw.csv')
    print("-------Data Saved------")

# proses cleaning
def cleanData():

    # Load data dari hasil PostgreSQL
    df = pd.read_csv('/opt/airflow/dags/data_raw.csv')

    # 1. Normalisasi nama kolom
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'\s+', '_', regex=True)
        .str.replace(r'[^\w]', '', regex=True)
    )

    # 2. Handling missing values
    df.dropna(inplace=True)

    # 3. Menghapus data duplikat
    df.drop_duplicates(inplace=True)

    # 4. Menyimpan hasil data cleaning
    df.to_csv(
        '/opt/airflow/dags/data_clean.csv',
        index=False
    )

#load data ke elasticsearch
def load_elasticsearch():
    df = pd.read_csv(f'/opt/airflow/dags/data_clean.csv')
    es = Elasticsearch(['http://elasticsearch:9200'])
    actions = (
        {'_index': 'customer_shopping', '_source': row}
        for row in df.to_dict(orient='records')
    )
    success, _ = helpers.bulk(es, actions, stats_only=True)
    print(f'Dokumen berhasil dimuat: {success}')

default_args = {
    'owner': 'amanda',
    'start_date': dt.datetime(2026, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG('FromPostgre',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5)
         ,catchup=False      # '0 * * * *',
         ) as dag:
    fetch = PythonOperator(task_id='get_data_postgre', python_callable=queryPostgresql)
    clean = PythonOperator(task_id='clean_data', python_callable=cleanData)
    load = PythonOperator(task_id='load_elasticsearch', python_callable=load_elasticsearch)

    fetch >> clean >> load