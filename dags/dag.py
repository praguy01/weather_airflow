#dag - directed acyclic graph
#ETL
#tasks : 1.fetch waether_api 2.clean data 3.create and store data on postgres
import requests
import os
import pandas as pd
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_dag',
    description='A simple DAG to fetch and clean weather data',
    schedule='@daily', 
    catchup=False,
    default_args=default_args
)

#1
def fetch_weather_data(ti):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 13.75,  # Bangkok
        "longitude": 100.5,
        "hourly": "temperature_2m",
        "timezone": "Asia/Bangkok"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # push raw JSON to XCom
    ti.xcom_push(key='raw_weather_data', value=data)

#2
def clean_weather_data(ti):
    data = ti.xcom_pull(key='raw_weather_data', task_ids='fetch_weather_data')

    hourly = data.get('hourly', {})
    times = hourly.get('time', [])
    temps = hourly.get('temperature_2m', [])

    if not times or not temps:
        raise ValueError("Missing time or temperature data")

    cleaned = [
        {'timestamp': t, 'temperature': temp}
        for t, temp in zip(times, temps)
    ]

    # push cleaned data to XCom
    ti.xcom_push(key='cleaned_data', value=cleaned)

#3
def load_weather_data(ti):
    records = ti.xcom_pull(key='cleaned_data', task_ids='clean_weather_data')
    if not records:
        print("No records received from XCom.")
        return

    print(f"Records received: {len(records)}")

    hook = PostgresHook(postgres_conn_id='waether_connec') 
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            timestamp TIMESTAMP PRIMARY KEY,
            temperature FLOAT
        );
    """)

    for rec in records:
      timestamp = datetime.fromisoformat(rec['timestamp'])
      cursor.execute("""
        INSERT INTO weather_data (timestamp, temperature)
        VALUES (%s, %s)
        ON CONFLICT (timestamp) DO NOTHING;
      """, (timestamp, rec['temperature']))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(records)} records into PostgreSQL.")



# Task 1: ดึงข้อมูล
task_fetch = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

# Task 2:  clean ข้อมูล
clean_task = PythonOperator(
    task_id='clean_weather_data',
    python_callable=clean_weather_data,
    dag=dag
)

# Task 3: บันทึกข้อมูล
load_task = PythonOperator(
    task_id='load_cleaned_data',
    python_callable=load_weather_data,
    dag=dag
)

# กำหนดลำดับการทำงาน
task_fetch >> clean_task >> load_task

# if __name__ == "__main__":
#     fetch_weather_data()
#     #clean_weather_data()
#     #load_weather_data()

