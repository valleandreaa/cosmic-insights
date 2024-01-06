from data_transform.news.sentiment_trend import get_sentiment
from datetime import datetime
from decimal import Decimal
from psycopg2 import sql
import json
import psycopg2

# Database connection parameters
db_params = {
    'dbname': 'news_db',
    'user': 'postgres',
    'password': 'andrea',
    'host': 'localhost',  # Adjust as per your configuration
    'port': '5433',
}

connection = None

# JSON file path
date = "2023-12-19T14"
json_file_path = f"../data_collection/space-news-data-all-{date}.json"  # adjust path

# SQL Queries
insert_time_query = sql.SQL("""
    INSERT INTO dim_time (year, quarter, month, week, day, hour, minute)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_time_unique_values
    DO UPDATE SET year = EXCLUDED.year
    RETURNING time_id;
""")

insert_type_query = sql.SQL("""
    INSERT INTO dim_type (type_name)
    VALUES (%s)
    ON CONFLICT ON CONSTRAINT uq_dim_type_unique_values
    DO UPDATE SET type_name = EXCLUDED.type_name
    RETURNING type_id;
""")

insert_sentiment_query = sql.SQL("""
    INSERT INTO dim_sentiment (score)
    VALUES (%s)
    ON CONFLICT ON CONSTRAINT uq_dim_sentiment_unique_values
    DO UPDATE SET score = EXCLUDED.score
    RETURNING sentiment_id;
""")

insert_source_query = sql.SQL("""
    INSERT INTO dim_source (source_name)
    VALUES (%s)
    ON CONFLICT ON CONSTRAINT uq_dim_source_unique_values
    DO UPDATE SET source_name = EXCLUDED.source_name
    RETURNING source_id;
""")

insert_news_detail_query = sql.SQL("""
    INSERT INTO dim_news_detail (title, summary, url, img_url)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_dim_news_detail_unique_values
    DO UPDATE SET title = EXCLUDED.title,
                  summary = EXCLUDED.summary,
                  url = EXCLUDED.url,
                  img_url = EXCLUDED.img_url
    RETURNING news_id;
""")

insert_data_query = sql.SQL("""
    INSERT INTO fact_news (news_id, time_id, source_id, type_id, sentiment_id)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT uq_fact_sheet_unique_values
    DO NOTHING;
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
    full_date = data.get('published_at', '')
    formats_to_try = ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ']

    date_obj = None
    for fmt in formats_to_try:
        try:
            date_obj = datetime.strptime(full_date, fmt)
            break  # Break if successful
        except ValueError:
            pass  # Try the next format

    year, month, day, hour, minute = date_obj.year, date_obj.month, date_obj.day, date_obj.hour, date_obj.minute
    quarter = (month - 1) // 3 + 1
    iso_calendar = date_obj.isocalendar()
    iso_year, iso_week, iso_weekday = iso_calendar
    cursor.execute(insert_time_query, (year, quarter, month, iso_week, day, hour, minute))
    time_id = cursor.fetchone()[0]
    return time_id


def insert_type(data, cursor):
    cursor.execute(insert_type_query, (data,))
    type_id = cursor.fetchone()[0]
    return type_id


def insert_sentiment(data, cursor):
    summary = data.get('summary', '')
    if summary != '':
        sentiment = get_sentiment(summary)
    else:
        sentiment = float(0)
    cursor.execute(insert_sentiment_query, (sentiment,))
    sentiment_id = cursor.fetchone()[0]
    return sentiment_id


def insert_source(data, cursor):
    cursor.execute(insert_source_query, (data.get('news_site', ''),))
    source_id = cursor.fetchone()[0]
    return source_id


def insert_news_detail(data, cursor):
    cursor.execute(insert_news_detail_query, (
        data.get('title', ''),
        data.get('summary', ''),
        data.get('url', ''),
        data.get('image_url', '')
    ))
    news_id = cursor.fetchone()[0]
    return news_id


def insert_data(data_type, data):
    try:
        cursor = connection.cursor()
        time_id = insert_time(data, cursor)
        type_id = insert_type(data_type, cursor)
        sentiment_id = insert_sentiment(data, cursor)
        source_id = insert_source(data, cursor)
        news_id = insert_news_detail(data, cursor)

        cursor.execute(insert_data_query, (news_id, time_id, source_id, type_id, sentiment_id))
        connection.commit()
        print("Data successfully inserted into the database.")
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
            for entry_type, entries in original_data.items():
                for entry in entries:
                    insert_data(entry_type, entry)
        connection.close()
