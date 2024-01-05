from datetime import datetime
from decimal import Decimal
from psycopg2 import sql
import json
import psycopg2

# Database connection parameters
db_params = {
    'dbname': 'neo_db',
    'user': 'postgres',
    'password': 'andrea',
    'host': 'localhost',  # 192.168.1.26
    'port': '5433',
}

connection = None

# JSON file path
json_file_path = 'nasa-data-2023-12-17.json'  # adjust path

insert_time_query = sql.SQL("""
    INSERT INTO dim_time (year, quarter, month, week, day, hour, minute)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_time_unique_values
    DO UPDATE SET year = EXCLUDED.year
    RETURNING time_id;
""")

insert_magnitude_query = sql.SQL("""
    INSERT INTO dim_magnitude (magnitude)
    VALUES (%s)
    ON CONFLICT ON CONSTRAINT uq_dim_magnitude_unique_values
    DO UPDATE SET magnitude = EXCLUDED.magnitude
    RETURNING magnitude_id;
""")

insert_type_query = sql.SQL("""
    INSERT INTO dim_type (type_name, description, class_range)
    VALUES (%s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_type_unique_values
    DO UPDATE SET type_name = EXCLUDED.type_name
    RETURNING type_id;
""")

insert_diameter_query = sql.SQL("""
    INSERT INTO dim_diameter (kilometer, meter, miles, feet)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_diameter_unique_values
    DO UPDATE SET kilometer = EXCLUDED.kilometer
    RETURNING diameter_id;
""")

insert_asteroid_detail_query = sql.SQL("""
    INSERT INTO dim_asteroid_detail (is_potentially_hazardous, is_sentry_object, arc_in_days,
    minimum_orbit_intersection, epoch_osculation, eccentricity, semi_major_axis, inclination, ascending_node_longitude,
    orbital_period, perihelion_distance, perihelion_argument, mean_anomaly, mean_motion, orbit_determination_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_asteroid_unique_values
    DO UPDATE SET is_potentially_hazardous = EXCLUDED.is_potentially_hazardous
    RETURNING asteroid_id;
""")


def create_connection():
    """Create a database connection."""
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"Error: Unable to connect to the database. {e}")
        return None


def insert_time(data, cursor):
    close_approach_data = data.get('close_approach_data', [])
    time_ids = []
    if len(close_approach_data) > 0:
        for approach_data in close_approach_data:
            full_date = approach_data['close_approach_date_full']
            date_obj = datetime.strptime(full_date, "%Y-%b-%d %H:%M")
            year, month, day, hour, minute = date_obj.year, date_obj.month, date_obj.day, date_obj.hour, date_obj.minute
            quarter = (month - 1) // 3 + 1

            iso_calendar = date_obj.isocalendar()
            iso_year, iso_week, iso_weekday = iso_calendar

            cursor.execute(insert_time_query, (year, quarter, month, iso_week, day, hour, minute))
            time_id = cursor.fetchone()[0]
            time_ids.append(time_id)
        return time_ids
    return None


def insert_magnitude(data, cursor):
    magnitude_value = data.get('absolute_magnitude_h', 0.0)
    cursor.execute(insert_magnitude_query, (magnitude_value,))
    magnitude_id = cursor.fetchone()[0]
    return magnitude_id


def insert_type(data, cursor):
    orbit_type = data.get('orbit_class', {})
    type_id = None
    if orbit_type:
        cursor.execute(insert_type_query, (orbit_type.get('orbit_class_type', ""),
                                           orbit_type.get('orbit_class_description', ""),
                                           orbit_type.get('orbit_class_range', "")))
        type_id = cursor.fetchone()[0]
    return type_id


def insert_diameter(data, cursor):
    diameter = data.get('estimated_diameter', {})
    kilometers = sum(diameter.get('kilometers', {'min': 0, 'max': 0}).values()) / 2
    meters = sum(diameter.get('meters', {'min': 0, 'max': 0}).values()) / 2
    miles = sum(diameter.get('miles', {'min': 0, 'max': 0}).values()) / 2
    feet = sum(diameter.get('feet', {'min': 0, 'max': 0}).values()) / 2
    cursor.execute(insert_diameter_query, (kilometers, meters, miles, feet))
    diameter_id = cursor.fetchone()[0]
    return diameter_id


def insert_asteroid_detail(danger, sentry, data, cursor):
    values = (danger, sentry, data.get('data_arc_in_days', 0), data.get('minimum_orbit_intersection', 0),
              data.get('epoch_osculation', 0), data.get('eccentricity', 0), data.get('semi_major_axis', 0),
              data.get('inclination', 0), data.get('ascending_node_longitude', 0), data.get('orbital_period', ""),
              data.get('perihelion_distance', 0), data.get('perihelion_argument', 0),
              data.get('mean_anomaly', 0), data.get('mean_motion', 0), data.get('orbit_determination_date'))
    cursor.execute(insert_asteroid_detail_query, values)
    asteroid_id = cursor.fetchone()[0]
    return asteroid_id


def insert_data(data):
    """Insert data into the fact_sheet table."""
    try:
        cursor = connection.cursor()

        orbital_data = data.get('orbital_data', {})

        # Insert
        time_ids = insert_time(data, cursor)
        magnitude_id = insert_magnitude(data, cursor)
        type_id = insert_type(orbital_data, cursor)
        diameter_id = insert_diameter(data, cursor)
        asteroid_id = insert_asteroid_detail(data.get('is_potentially_hazardous_asteroid', False),
                                             data.get('is_sentry_object', False), orbital_data, cursor)

        # Insert data into the fact_sheet table
        if time_ids is not None:
            for time_id in time_ids:
                cursor.execute("""
                    INSERT INTO fact_asteroid (time_id, diameter_id, magnitude_id, asteroid_id, type_id, asteroid_name)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT uq_fact_sheet_unique_values DO NOTHING;
                """, (time_id, diameter_id, magnitude_id, asteroid_id, type_id, data.get('name')))
        else:
            time_id = None
            cursor.execute("""
                INSERT INTO fact_asteroid (time_id, diameter_id, magnitude_id, asteroid_id, type_id, asteroid_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT uq_fact_sheet_unique_values DO NOTHING;
            """, (time_id, diameter_id, magnitude_id, asteroid_id, type_id, data.get('name')))
        connection.commit()
        print("Data successfully inserted into the database.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error: Unable to insert data into the database. {e}")


def read_json_file(file_path):
    """Read JSON file."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file, parse_float=Decimal)
    except Exception as e:
        print(f"Error: Unable to read JSON file. {e}")
        return None


if __name__ == "__main__":
    connection = create_connection()
    if connection:
        original_data = read_json_file(json_file_path)
        if original_data:
            for entry in original_data:
                insert_data(entry)
        connection.close()
