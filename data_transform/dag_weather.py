from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
import psycopg2
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import logging
import json

# Function to read JSON data from S3
def read_json_from_s3(*args, **kwargs):
    ti = kwargs['ti']
    bucket_name = kwargs['bucket_name']
    key = kwargs['key']

    s3_hook = S3Hook(aws_conn_id='aws_asteroids')
    file_object = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)
    file_content = file_object.get('Body').read().decode('utf-8')
    json_data = json.loads(file_content)
    ti.xcom_push(key='json_data', value=json_data)
    

# Function to insert news details into dim_news_detail
def insert_into_dim_time(*args, **kwargs):
    # Extracting time-related data from the JSON payload
    json_data = kwargs['ti'].xcom_pull(key='json_data', task_ids='read_from_s3')
    # Assume 'time_tag' is a key in your JSON data. Adjust as necessary.
    time_tags = [record[0] for record in json_data['mag'][1:]]
    print(time_tags)
    conn = BaseHook.get_connection('rds_postgres_conn_weather').get_uri()
    time_ids = []

    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            for time_tag in time_tags:
                # Converting time_tag to datetime object
                observation_datetime = datetime.fromisoformat(time_tag)

                year = observation_datetime.year
                month = observation_datetime.month
                day = observation_datetime.day
                hour = observation_datetime.hour
                minute = observation_datetime.minute
                quarter = (month - 1) // 3 + 1
                week = observation_datetime.isocalendar()[1]

                cursor.execute("""
                    INSERT INTO dim_time (year, quarter, month, week, day, hour, minute)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT uq_dim_time_unique_values
                    DO UPDATE SET year = EXCLUDED.year
                    RETURNING time_id;
                """, (year, quarter, month, week, day, hour, minute))
                result = cursor.fetchone()[0]
                
                time_ids.append(result)
            connection.commit()
    return time_ids

def insert_into_dim_mag(*args, **kwargs):
    mag_data = kwargs['ti'].xcom_pull(key='json_data', task_ids='read_from_s3')["mag"]
    conn = BaseHook.get_connection('rds_postgres_conn_weather').get_uri()
    mag_ids = []

    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            for record in mag_data[1:]:
                bx_gsm, by_gsm, bz_gsm = record[1:4]
                cursor.execute("""
                    INSERT INTO dim_mag (bx_gsm, by_gsm, bz_gsm) VALUES (%s, %s, %s)
                    ON CONFLICT ON CONSTRAINT uq_dim_mag_unique_values
                    DO UPDATE SET bx_gsm = EXCLUDED.bx_gsm
                    RETURNING mag_id;
                """, (bx_gsm, by_gsm, bz_gsm))
                result = cursor.fetchone()
                
                mag_ids.append(result[0])
            connection.commit()
    return mag_ids

def insert_into_dim_plasma(*args, **kwargs):
    plasma_data = kwargs['ti'].xcom_pull(key='json_data', task_ids='read_from_s3')["plasma"]
    conn = BaseHook.get_connection('rds_postgres_conn_weather').get_uri()
    plasma_ids = []

    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            for record in plasma_data[1:]:  # Skip the header row
                density, speed, temperature = record[1:4]
                cursor.execute("""
                    INSERT INTO dim_plasma (density, speed, temperature)
                    VALUES (%s, %s, %s)
                    ON CONFLICT ON CONSTRAINT uq_dim_plasma_unique_values
                    DO UPDATE SET density = EXCLUDED.density
                    RETURNING plasma_id;
                """, (density, speed, temperature))
                result = cursor.fetchone()
                
                plasma_ids.append(result[0])
            connection.commit()
    return plasma_ids

def insert_into_dim_magnetometer(*args, **kwargs):
    magnetometer_data = kwargs['ti'].xcom_pull(key='json_data', task_ids='read_from_s3')["magnetometers"]
    conn = BaseHook.get_connection('rds_postgres_conn_weather').get_uri()
    magnetometer_ids = []

    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            for record in magnetometer_data:
                He = record["He"]
                Hp = record["Hp"]
                Hn = record["Hn"]
                total = record["total"]

                cursor.execute("""
                    INSERT INTO dim_magnetometer (he, hp, hn, total)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT uq_dim_magnetometer_unique_values
                    DO UPDATE SET he = EXCLUDED.he
                    RETURNING magnetometer_id;
                """, (He, Hp, Hn, total))
                result = cursor.fetchone()
                
                magnetometer_ids.append(result[0])
            connection.commit()
    return magnetometer_ids

def insert_into_dim_proton(*args, **kwargs):
    protons_data = kwargs['ti'].xcom_pull(key='json_data', task_ids='read_from_s3')["protons"]
    conn = BaseHook.get_connection('rds_postgres_conn_weather').get_uri()
    proton_ids = []

    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            for record in protons_data:
                flux = record["flux"]
                energy_code = get_energy_code(record["energy"])

                cursor.execute("""
                    INSERT INTO dim_proton (flux, energy)
                    VALUES (%s, %s)
                    ON CONFLICT ON CONSTRAINT uq_dim_proton_unique_values
                    DO UPDATE SET flux = EXCLUDED.flux
                    RETURNING proton_id;
                """, (flux, energy_code))
                result = cursor.fetchone()
                
                proton_ids.append(result[0])
            connection.commit()
    return proton_ids

def get_energy_code(energy_string):
    # Implement a method to convert energy string to a numeric code
    # Example implementation (adapt based on your specific energy level strings):
    energy_mapping = {
        ">=1 MeV": 1,
        ">=10 MeV": 10,
        ">=5 MeV":5,
        ">=30 MeV":30,
        ">=100 MeV":100,
        ">=50 MeV":50,
        ">=60 MeV":60,
        ">=500 MeV":500
        # Add other mappings as necessary
    }
    return energy_mapping.get(energy_string, None)  # Returns None if the energy string is not in the mapping

def insert_into_fact_weather(*args, **kwargs):
    ti = kwargs['ti']
    time_ids = ti.xcom_pull(task_ids='insert_dim_time')
    mag_ids = ti.xcom_pull(task_ids='insert_dim_mag')
    plasma_ids = ti.xcom_pull(task_ids='insert_dim_plasma')
    magnetometer_ids = ti.xcom_pull(task_ids='insert_dim_magnetometer')
    proton_ids = ti.xcom_pull(task_ids='insert_dim_proton')
    
    conn = BaseHook.get_connection('rds_postgres_conn_weather').get_uri()

    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            # Check if all lists are of equal length before proceeding
            
            for i in range(len(time_ids)):
                try:
                    time_id = time_ids[i]
                    mag_id = mag_ids[i]
                    plasma_id = plasma_ids[i]
                    magnetometer_id = magnetometer_ids[i]
                    proton_id = proton_ids[i]

                    cursor.execute("""
                        INSERT INTO fact_weather (time_id, mag_id, plasma_id, magnetometer_id, proton_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (time_id, mag_id, plasma_id, magnetometer_id, proton_id))
                    connection.commit()
                except:
                    pass

# Additional functions to insert data into dim_time, dim_source, dim_type, dim_sentiment, and fact_news
# should be defined here, similar to the insert_into_news_detail function.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='weather_airflow',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2023, 3, 1),
        catchup=False
) as dag:
    read_from_s3 = PythonOperator(
        task_id='read_from_s3',
        python_callable=read_json_from_s3,
        op_kwargs={
            'key': 'weather-data-2023-12-20.json',
            'bucket_name': 'swagger23',
        }
    )



    insert_dim_mag = PythonOperator(
        task_id='insert_dim_mag',
        python_callable=insert_into_dim_mag,
        op_kwargs={}
    )

    # Task for inserting data into dim_plasma
    insert_dim_plasma = PythonOperator(
        task_id='insert_dim_plasma',
        python_callable=insert_into_dim_plasma,
        op_kwargs={}
    )

    # Task for inserting data into dim_magnetometer
    insert_dim_magnetometer = PythonOperator(
        task_id='insert_dim_magnetometer',
        python_callable=insert_into_dim_magnetometer,
        op_kwargs={}
    )

    # Task for inserting data into dim_proton
    insert_dim_proton = PythonOperator(
        task_id='insert_dim_proton',
        python_callable=insert_into_dim_proton,
        op_kwargs={}
    )

    # Task for inserting data into fact_weather (to be implemented)
    insert_fact_weather = PythonOperator(
        task_id='insert_fact_weather',
        python_callable=insert_into_fact_weather,
        op_kwargs={}
    )

    insert_dim_time = PythonOperator(
    task_id='insert_dim_time',
    python_callable=insert_into_dim_time,
    op_kwargs={}
    )


# Setting up dependencies
read_from_s3 >> [insert_dim_time,  insert_dim_mag, insert_dim_plasma, insert_dim_magnetometer, insert_dim_proton]
[insert_dim_time,  insert_dim_mag, insert_dim_plasma, insert_dim_magnetometer, insert_dim_proton] >> insert_fact_weather
