from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.operators.python_operator import PythonOperator
import psycopg2
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import logging
import json
from datetime import datetime, timedelta

# Function to read JSON data from an S3 bucket
def read_json_from_s3(*args, **kwargs):
    # Extracting task instance and required parameters
    ti = kwargs['ti']
    bucket_name = kwargs['bucket_name']
    key = kwargs['key']

    # Creating an S3 hook and reading the file content
    s3_hook = S3Hook(aws_conn_id='aws_asteroids')
    file_object = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)
    file_content = file_object.get('Body').read().decode('utf-8')
    json_data = json.loads(file_content)

    # Pushing the data to XCom for the next task
    ti.xcom_push(key='json_data', value=json_data)

    logging.info(f"Read content from {key} in S3 bucket {bucket_name}")

# Inserting time data into the database
def insert_into_time_dim(*args, **kwargs):
    # Extraction from XCom
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    time_list = []

    # Parsing time data
    for asteroid in near_earth_objects:
        close_approach_data = asteroid.get('close_approach_data', [])

        for approach in close_approach_data:
            observation_timestamp = approach.get('close_approach_date_full')

            if observation_timestamp:

                observation_datetime = datetime.strptime(observation_timestamp, '%Y-%b-%d %H:%M')
                time_list.append([
                    observation_datetime.year,
                    observation_datetime.month,
                    (observation_datetime.month - 1) // 3 + 1,
                    observation_datetime.isocalendar()[1],
                    observation_datetime.day,
                    observation_datetime.hour,
                    observation_datetime.minute
                ])

    # Database connection and insertion
    time_ids = []
    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO dim_time (year, quarter, month, week, day, hour, minute) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uq_dim_time_unique_values
            DO UPDATE SET year = EXCLUDED.year
            RETURNING time_id;
            """
            for time_item in time_list:
                cursor.execute(insert_query, time_item)
                time_id = cursor.fetchone()[0]
                time_ids.append(time_id)
            connection.commit()
    return time_ids

# Inserting asteroids data into the database
def insert_into_asteroids(*args, **kwargs):
    # Extraction from XCom
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')

    # Parsing asteroids data
    asteroids_list = []
    asteroids_names = []
    for asteroid in near_earth_objects:

        # Parsing asteroids data
        orbital_data = asteroid.get('orbital_data', {})

        asteroids_list.append([
            asteroid.get('is_potentially_hazardous_asteroid'),
            asteroid.get('is_sentry_object'),
            orbital_data.get('data_arc_in_days'),
            orbital_data.get('minimum_orbit_intersection'),
            orbital_data.get('epoch_osculation'),
            orbital_data.get('eccentricity'),
            orbital_data.get('semi_major_axis'),
            orbital_data.get('inclination'),
            orbital_data.get('ascending_node_longitude'),
            orbital_data.get('orbital_period'),
            orbital_data.get('perihelion_distance'),
            orbital_data.get('perihelion_argument'),
            orbital_data.get('mean_anomaly'),
            orbital_data.get('mean_motion'),
            orbital_data.get('orbit_determination_date')
        ])

        asteroids_names.append(asteroid.get('name'))

    logging.info(f"Asteroids")

    # Database connection and insertion
    inserted_records = []
    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO dim_asteroid_detail (is_potentially_hazardous, is_sentry_object, arc_in_days, minimum_orbit_intersection, epoch_osculation, eccentricity, semi_major_axis, inclination, ascending_node_longitude, orbital_period, perihelion_distance, perihelion_argument, mean_anomaly, mean_motion, orbit_determination_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uq_dim_asteroid_unique_values
            DO UPDATE SET is_potentially_hazardous = EXCLUDED.is_potentially_hazardous
            RETURNING asteroid_id;
            """

            for idx, asteroid in enumerate(asteroids_list):
                cursor.execute(insert_query, asteroid)
                inserted_id = cursor.fetchone()[0]
                asteroid_name = asteroids_names[idx]  # Assuming the name is the first element in the asteroid list
                inserted_records.append((inserted_id, asteroid_name))
            connection.commit()
    return inserted_records


def insert_into_diameter(*args, **kwargs):
    # Extraction from XCom
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')

    # Parsing diameter data
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

    # Database connection and insertion
    diameter_ids = []
    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:

            insert_query = """
            INSERT INTO dim_diameter (kilometer, meter, miles, feet ) VALUES ( %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uq_dim_diameter_unique_values
            DO UPDATE SET kilometer = EXCLUDED.kilometer
            RETURNING diameter_id;  
            """

            for diameter in diameter_list:
                cursor.execute(insert_query, diameter)
                diameter_id = cursor.fetchone()[0]
                diameter_ids.append(diameter_id)

            connection.commit()

    return diameter_ids


def insert_into_type(*args, **kwargs):
    # Extraction from XCom
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')

    # Parsing type data
    type_list = []
    for asteroid in near_earth_objects:

        orbit_class = asteroid['orbital_data']['orbit_class']
        orbit_class_type = orbit_class.get('orbit_class_type')
        orbit_class_range = orbit_class.get('orbit_class_range')

        type_list.append([orbit_class_type, orbit_class_range])

    # Database connection and insertion
    type_ids = []
    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()
    print()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:

            insert_query = """
            INSERT INTO dim_type (type_name, description) VALUES (%s, %s)
            ON CONFLICT ON CONSTRAINT uq_dim_type_unique_values
            DO UPDATE SET type_name = EXCLUDED.type_name
            RETURNING type_id;
            """

            for type_item in type_list:
                cursor.execute(insert_query, type_item)
                type_id = cursor.fetchone()[0]
                type_ids.append(type_id)

            connection.commit()

    return type_ids


def insert_into_magnitude(*args, **kwargs):
    # Extraction from XCom
    ti = kwargs['ti']
    near_earth_objects = ti.xcom_pull(key='json_data', task_ids='read_from_s3')

    # Parsing magnitude data
    magnitude_list = []
    for asteroid in near_earth_objects:
        magnitude_value = asteroid.get('absolute_magnitude_h')
        magnitude_list.append([magnitude_value])

    # Database connection and insertion
    magnitude_ids = []
    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()

    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:

            insert_query = """
            INSERT INTO dim_magnitude (magnitude) VALUES (%s)
            ON CONFLICT ON CONSTRAINT uq_dim_magnitude_unique_values
            DO UPDATE SET magnitude = EXCLUDED.magnitude
            RETURNING magnitude_id;
            """

            for magnitude in magnitude_list:
                cursor.execute(insert_query, magnitude)
                magnitude_id = cursor.fetchone()[0]
                magnitude_ids.append(magnitude_id)

            connection.commit()

    return magnitude_ids


def insert_into_fact_asteroid(*args, **kwargs):

    # Extraction from XCom
    ti = kwargs['ti']
    asteroid_records = ti.xcom_pull(task_ids='transfer_s3_to_sql_asteroids')
    diameter_ids = ti.xcom_pull(task_ids='transfer_s3_to_sql_diameter')
    type_ids = ti.xcom_pull(task_ids='transfer_s3_to_sql_type')
    magnitude_ids = ti.xcom_pull(task_ids='transfer_s3_to_sql_magnitude')
    time_ids = ti.xcom_pull(task_ids='transfer_s3_to_sql_time_dim')

    # Combine the IDs and names to form fact_asteroid records
    fact_asteroid_records = []
    for i, asteroid in enumerate(asteroid_records):
        asteroid_id, asteroid_name = asteroid
        time_id = time_ids[i]
        diameter_id = diameter_ids[i]
        type_id = type_ids[i]
        magnitude_id = magnitude_ids[i]
        fact_asteroid_records.append((time_id, diameter_id, magnitude_id, asteroid_id, type_id, asteroid_name))

    # Database connection and insertion
    conn = BaseHook.get_connection('rds_postgres_conn').get_uri()

    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:

            insert_query = """
            INSERT INTO fact_asteroid (time_id, diameter_id, magnitude_id, asteroid_id, type_id, asteroid_name)
            VALUES (%s, %s, %s, %s, %s, %s);
            """

            cursor.executemany(insert_query, fact_asteroid_records)
            connection.commit()

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='neo_airflow_2',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2023, 3, 1),
        catchup=False
) as dag:
    today_date = datetime.now().strftime('%Y-%m-%d')

    # Task definitions
    read_from_s3 = PythonOperator(
        task_id='read_from_s3',
        python_callable=read_json_from_s3,
        op_kwargs={
            'key': f'nasa-data-{today_date}-2.json',
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

    transfer_s3_to_sql_time_dim = PythonOperator(
        task_id='transfer_s3_to_sql_time_dim',
        python_callable=insert_into_time_dim,
        op_kwargs={}
    )

    transfer_fact_asteroid = PythonOperator(
        task_id='transfer_fact_asteroid',
        python_callable=insert_into_fact_asteroid,
        op_kwargs={}
    )

# Setting up dependencies among the task
read_from_s3 >> [transfer_s3_to_sql_time_dim, transfer_s3_to_sql_asteroids, transfer_s3_to_sql_diameter,
                 transfer_s3_to_sql_type, transfer_s3_to_sql_magnitude]
[transfer_s3_to_sql_time_dim, transfer_s3_to_sql_asteroids, transfer_s3_to_sql_diameter, transfer_s3_to_sql_type,
 transfer_s3_to_sql_magnitude] >> transfer_fact_asteroid