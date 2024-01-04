from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.operators.python_operator import PythonOperator
import psycopg2
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import logging
import json


def read_json_from_s3(*args, **kwargs):
    ti = kwargs['ti']
    bucket_name = kwargs['bucket_name']
    key = kwargs['key']

    s3_hook = S3Hook(aws_conn_id='aws_asteroids')
    file_object = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)
    file_content = file_object.get('Body').read().decode('utf-8')
    json_data = json.loads(file_content)
    print(json_data)
    # Push the data to XCom for the next task to use
    ti.xcom_push(key='json_data', value=json_data)

    logging.info(f"Read content from {key} in S3 bucket {bucket_name}")


def custom_s3_to_sql_transfer_asteroids(json_data):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='json_data', task_ids='read_json_from_s3')
    return asteroids_list


def insert_into_asteroids(*args, **kwargs):
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    # asteroids_list = custom_s3_to_sql_transfer_asteroids(data)

    asteroids_list = []

    for asteroid in near_earth_objects:
        # Extracting data from the 'orbital_data' nested dictionary
        orbital_data = asteroid.get('orbital_data', {})

        asteroids_list.append([
            # asteroid.get('id'),
            asteroid.get('name'),
            asteroid.get('is_potentially_hazardous_asteroid'),
            asteroid.get('is_sentry_object'),
            orbital_data.get('data_arc_in_days'),
            orbital_data.get('observations_used'),
            orbital_data.get('orbit_uncertainty'),
            orbital_data.get('minimum_orbit_intersection'),
            orbital_data.get('epoch_osculation'),
            orbital_data.get('eccentricity'),
            orbital_data.get('semi_major_axis'),
            orbital_data.get('inclination'),
            orbital_data.get('ascending_node_longitude'),
            orbital_data.get('orbital_period'),
            orbital_data.get('perihelion_distance'),
            orbital_data.get('perihelion_argument'),
            orbital_data.get('aphelion_distance'),
            orbital_data.get('perihelion_time'),
            orbital_data.get('mean_anomaly'),
            orbital_data.get('mean_motion'),
            orbital_data.get('equinox'),
            orbital_data.get('orbit_determination_date')
        ])
    logging.info(f"Asteroids")

    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO asteroid (
                 name, is_potentially_hazardous, is_sentry_object, 
                arc_in_days, observations_used, orbit_uncertainty, minimum_orbit_intersection,
                epoch_osculation, eccentricity, semi_major_axis, inclination, 
                ascending_node_longitude, orbital_period, perihelion_distance, 
                perihelion_argument, aphelion_distance, perihelion_time, 
                mean_anomaly, mean_motion, equinox, orbit_determination_date
            ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            cursor.executemany(insert_query, asteroids_list)
            connection.commit()


def insert_into_diameter(*args, **kwargs):
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    diameter_list = []

    for asteroid in near_earth_objects:
        diameter_kilometers = asteroid['estimated_diameter']['kilometers']
        diameter_meters = asteroid['estimated_diameter']['meters']
        diameter_miles = asteroid['estimated_diameter']['miles']
        diameter_feet = asteroid['estimated_diameter']['feet']

        avg_diameter_km = (diameter_kilometers['estimated_diameter_max'] + diameter_kilometers[
            'estimated_diameter_min']) / 2
        avg_diameter_m = (diameter_meters['estimated_diameter_max'] + diameter_meters['estimated_diameter_min']) / 2
        avg_diameter_miles = (diameter_miles['estimated_diameter_max'] + diameter_miles['estimated_diameter_min']) / 2
        avg_diameter_feet = (diameter_feet['estimated_diameter_max'] + diameter_feet['estimated_diameter_min']) / 2

        diameter_list.append([
            avg_diameter_km,
            avg_diameter_m,
            avg_diameter_miles,
            avg_diameter_feet
        ])

    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            # SQL query for inserting data into the diameter table
            insert_query = """
                    INSERT INTO diameter (
                         kilometer, meter, miles, feet
                    ) VALUES ( %s, %s, %s, %s)
                    """

            # Executing the query for each item in diameter_list
            cursor.executemany(insert_query, diameter_list)

            # Committing the transaction
            connection.commit()


def insert_into_type(*args, **kwargs):
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    type_list = []

    for asteroid in near_earth_objects:
        orbit_class = asteroid['orbital_data']['orbit_class']
        orbit_class_type = orbit_class.get('orbit_class_type')
        orbit_class_range = orbit_class.get('orbit_class_range')

        type_list.append([orbit_class_type, orbit_class_range])

    # Database insertion with conflict handling
    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO type (description, class_range) VALUES (%s, %s)
            
            """
            cursor.executemany(insert_query, type_list)
            connection.commit()


def insert_into_magnitude(*args, **kwargs):
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    magnitude_list = []

    for asteroid in near_earth_objects:
        magnitude_value = asteroid.get('absolute_magnitude_h')
        magnitude_list.append([magnitude_value])

    # Database insertion with conflict handling
    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO magnitude (magnitude) VALUES (%s)
            """
            cursor.executemany(insert_query, magnitude_list)
            connection.commit()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='s3_download_3',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2023, 3, 1),
        catchup=False
) as dag:
    read_from_s3 = PythonOperator(
        task_id='read_from_s3',
        python_callable=read_json_from_s3,
        op_kwargs={
            'key': 'nasa-data-single-2023-12-17.json',
            'bucket_name': 'swagger23',
        }
    )

    transfer_s3_to_sql_asteroids = PythonOperator(
        task_id='transfer_s3_to_sql_asteroids',
        python_callable=insert_into_asteroids,
        op_kwargs={}
    )

    transfer_s3_to_sql_diameter = PythonOperator(
        task_id='transfer_s3_to_sql_diameter',
        python_callable=insert_into_diameter,
        op_kwargs={}
    )

    transfer_s3_to_sql_type = PythonOperator(
        task_id='transfer_s3_to_sql_type',
        python_callable=insert_into_type,
        op_kwargs={}
    )

    transfer_s3_to_sql_magnitude = PythonOperator(
        task_id='transfer_s3_to_sql_magnitude',
        python_callable=insert_into_magnitude,
        op_kwargs={}
    )

read_from_s3 >> [transfer_s3_to_sql_asteroids, transfer_s3_to_sql_diameter, transfer_s3_to_sql_type,
                 transfer_s3_to_sql_magnitude]
