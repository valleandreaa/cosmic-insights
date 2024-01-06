from datetime import datetime
from psycopg2 import sql
import pandas as pd
import psycopg2
import requests
import time

# Database connection parameters
db_params = {
    'dbname': 'weather_db',
    'user': 'postgres',
    'password': 'andrea',
    'host': 'localhost',  # 192.168.1.26
    'port': '5433',
}

connection = None

insert_time_query = sql.SQL("""
    INSERT INTO dim_time (year, quarter, month, week, day, hour, minute)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_time_unique_values
    DO UPDATE SET year = EXCLUDED.year
    RETURNING time_id;
""")

insert_plasma_query = sql.SQL("""
    INSERT INTO dim_plasma (density, speed, temperature)
    VALUES (%s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_plasma_unique_values
    DO UPDATE SET density = EXCLUDED.density
    RETURNING plasma_id;
""")

insert_magnetometer_query = sql.SQL("""
    INSERT INTO dim_magnetometer (he, hp, hn, total)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_magnetometer_unique_values
    DO UPDATE SET total = EXCLUDED.total
    RETURNING magnetometer_id;
""")

insert_mag_query = sql.SQL("""
    INSERT INTO dim_mag (bx_gsm, by_gsm, bz_gsm)
    VALUES (%s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_mag_unique_values
    DO UPDATE SET bx_gsm = EXCLUDED.bx_gsm
    RETURNING mag_id;
""")

insert_proton_query = sql.SQL("""
    INSERT INTO dim_proton (flux, energy)
    VALUES (%s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_proton_unique_values
    DO UPDATE SET flux = EXCLUDED.flux
    RETURNING proton_id;
""")


# Function to create a database connection
def create_connection():
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"Error: Unable to connect to the database. {e}")
        return None


def insert_time(data, cursor):
    year, month, day, hour, minute = data.year, data.month, data.day, data.hour, data.minute
    quarter = (month - 1) // 3 + 1

    iso_calendar = data.isocalendar()
    iso_year, iso_week, iso_weekday = iso_calendar

    cursor.execute(insert_time_query, (year, quarter, month, iso_week, day, hour, minute))
    time_id = cursor.fetchone()[0]

    return time_id


def insert_plasma(data, cursor):
    density = data["density"]

    if density is None:
        return None

    speed = data["speed"]
    temperature = data["temperature"]
    cursor.execute(insert_plasma_query, (density, speed, temperature))
    plasma_id = cursor.fetchone()[0]

    return plasma_id


def insert_magnetometer(data, cursor):
    he = data["He"]

    if he is None:
        return None

    hp = data["Hp"]
    hn = data["Hn"]
    total = data["total"]
    cursor.execute(insert_magnetometer_query, (he, hp, hn, total))
    magnetometer_id = cursor.fetchone()[0]

    return magnetometer_id


def insert_mag(data, cursor):
    bx_gsm = data["bx_gsm"]

    if bx_gsm is None:
        return None

    by_gsm = data["by_gsm"]
    bz_gsm = data["bz_gsm"]
    cursor.execute(insert_mag_query, (bx_gsm, by_gsm, bz_gsm))
    mag_id = cursor.fetchone()[0]

    return mag_id


def insert_proton(data, cursor):
    flux = data["flux"]

    if flux is None:
        return None

    energy = data["energy"]
    cursor.execute(insert_proton_query, (flux, energy))
    proton_id = cursor.fetchone()[0]

    return proton_id


# Function to insert data into the database
def insert_data(data):
    try:
        cursor = connection.cursor()
        time_id = insert_time(data['time_tag'], cursor)
        plasma_id = insert_plasma(data, cursor)
        magnetometer_id = insert_magnetometer(data, cursor)
        mag_id = insert_mag(data, cursor)
        proton_id = insert_proton(data, cursor)

        # Create a dictionary of values
        values_to_insert = {
            'time_id': time_id,
            'plasma_id': plasma_id,
            'magnetometer_id': magnetometer_id,
            'mag_id': mag_id,
            'proton_id': proton_id
        }

        # Filter out None values
        values_to_insert = {key: value for key, value in values_to_insert.items() if value is not None}

        if values_to_insert:
            # Create the column names string for the SQL statement
            columns = ', '.join(values_to_insert.keys())

            # Create the placeholder string for the VALUES part of the SQL statement
            placeholders = ', '.join(['%s' for _ in values_to_insert])

            cursor.execute(f"""
                            INSERT INTO fact_weather ({columns})
                            VALUES ({placeholders})
                            ON CONFLICT ON CONSTRAINT uq_fact_sheet_unique_values DO NOTHING;
                        """, list(values_to_insert.values()))

            connection.commit()
            print("Data successfully inserted into the database.")
    except Exception as e:
        connection.rollback()
        print(f"Error: Unable to insert data into the database. {e}")


def fetch_data(specific_url, sleep_duration=5):
    url = f"https://services.swpc.noaa.gov/{specific_url}"
    try:
        with requests.Session() as session:
            # Make the request
            response = session.get(url)
            response.raise_for_status()

            # Return the JSON data if successful
            return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"For {url}: {e.response.status_code}")
        print(f"Error response content: {e.response.text}")
        time.sleep(sleep_duration)
        print("Retrying...")
        return fetch_data(specific_url, sleep_duration * 2)


# Function to convert JSON data to Pandas DataFrame
def convert_to_dataframe(data):
    if not data:
        return pd.DataFrame()

    pd.set_option("display.precision", 2)
    df = pd.DataFrame(data[1:], columns=data[0])
    df['time_tag'] = pd.to_datetime(df['time_tag'].apply(convert_to_consistent_format))

    # Exclude 'time_tag' and 'energy'
    non_time_tag_columns = df.columns[(df.columns != 'time_tag') & (df.columns != 'energy')]

    df[non_time_tag_columns] = df[non_time_tag_columns].apply(pd.to_numeric, errors='coerce')
    df[non_time_tag_columns] = df[non_time_tag_columns].round(2)

    if 'energy' in df.columns:
        df['energy'] = df['energy'].str.extract(r'(\d+)').astype(float)

    return df


# Function to convert datetime string to consistent format
def convert_to_consistent_format(datetime_str):
    try:
        # Attempt to parse the datetime string
        dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            # Add more parsing formats as needed
            raise ValueError("Unsupported datetime format")

    # Format the datetime object
    formatted_datetime_str = dt.strftime('%Y-%m-%d %H:%M')
    return formatted_datetime_str


# Function to fetch data from NOAA services
def get_data():
    # Same structure like in Data Lake
    all_content = {
        "mag": fetch_data("products/solar-wind/mag-1-day.json"),
        "plasma": fetch_data("products/solar-wind/plasma-1-day.json"),
        "magnetometers": fetch_data("json/goes/primary/magnetometers-1-day.json"),
        "protons": fetch_data("json/goes/primary/integral-protons-1-day.json")
    }

    mag = convert_to_dataframe(all_content["mag"])
    plasma = convert_to_dataframe(all_content["plasma"])
    magnetometers = convert_to_dataframe(all_content["magnetometers"])
    protons = convert_to_dataframe(all_content["protons"])

    weather = pd.merge(mag, plasma, on='time_tag', how='outer')
    weather = pd.merge(weather, magnetometers, on='time_tag', how='outer')
    weather = pd.merge(weather, protons, on='time_tag', how='outer')
    weather = weather.sort_values(by='time_tag')

    return weather


if __name__ == "__main__":
    connection = create_connection()
    if connection:
        weather_dataset = get_data()
        grouped_data = weather_dataset.groupby('time_tag')
        for index, row in weather_dataset.iterrows():
            insert_data(row)
        connection.close()
