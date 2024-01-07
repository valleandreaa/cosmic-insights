from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import psycopg2
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import json
from textblob import TextBlob


# Function to read JSON data from an S3 bucket
def read_json_from_s3(*args, **kwargs):
    # Extract parameters from kwargs
    ti = kwargs['ti']
    bucket_name = kwargs['bucket_name']
    key = kwargs['key']

    # Create an S3 hook and read the file
    s3_hook = S3Hook(aws_conn_id='aws_asteroids')
    file_object = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)
    file_content = file_object.get('Body').read().decode('utf-8')
    json_data = json.loads(file_content)

    # Push the JSON data to XCom for use in subsequent tasks
    ti.xcom_push(key='json_data', value=json_data)

# Function to process and insert time data into the database
def insert_into_time(*args, **kwargs):
    ti = kwargs['ti']
    articles = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    time_ids = []

    # Connect to the database
    conn = BaseHook.get_connection('rds_postgres_conn_news').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            # Process each article
            for entry_type, article_list in articles.items():
                for article in  article_list:
                    # Try different datetime formats for parsing
                    formats_to_try = ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ']

                    published_at  = None
                    for fmt in formats_to_try:
                        try:
                            published_at  = datetime.strptime(article['published_at'], fmt)
                            break  
                        except ValueError:
                            pass

                    # Extract date and time components
                    year, month, day = published_at.year, published_at.month, published_at.day
                    hour, minute = published_at.hour, published_at.minute
                    quarter = (month - 1) // 3 + 1
                    week = published_at.isocalendar()[1]

                    # Insert data into the database
                    cursor.execute("""
                        INSERT INTO dim_time (year, quarter, month, week, day, hour, minute)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (year, quarter, month, week, day, hour, minute)
                        DO UPDATE SET year = EXCLUDED.year
                        RETURNING time_id;
                    """, (year, quarter, month, week, day, hour, minute))
                    time_id = cursor.fetchone()[0]
                    time_ids.append(time_id)

    return time_ids

# Function to determine the sentiment of a text using TextBlob
def get_sentiment(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity

# Function to process and insert sentiment data into the database
def insert_into_sentiment(*args, **kwargs):
    ti = kwargs['ti']
    articles = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    sentiment_ids = []

    # Connect to the database
    conn = BaseHook.get_connection('rds_postgres_conn_news').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            # Process each article
            for entry_type, article_list in articles.items():
                for article in  article_list:
                    summary = article.get('summary', '')
                    sentiment = get_sentiment(summary)

                    # Insert sentiment data into the database
                    cursor.execute("""
                        INSERT INTO dim_sentiment (score)
                        VALUES (%s)
                        ON CONFLICT (score)
                        DO UPDATE SET score = EXCLUDED.score
                        RETURNING sentiment_id;
                    """, (sentiment,))
                    sentiment_id = cursor.fetchone()[0]
                    sentiment_ids.append(sentiment_id)

    return sentiment_ids

# Function to process and insert source data into the database
def insert_into_source(*args, **kwargs):
    ti = kwargs['ti']
    articles = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    source_ids = []

    # Connect to the database
    conn = BaseHook.get_connection('rds_postgres_conn_news').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            # Process each article
            for entry_type, article_list in articles.items():
                for article in  article_list:
                    source_name = article.get('news_site', '')

                    cursor.execute("""
                        INSERT INTO dim_source (source_name)
                        VALUES (%s)
                        ON CONFLICT (source_name)
                        DO UPDATE SET source_name = EXCLUDED.source_name
                        RETURNING source_id;
                    """, (source_name,))
                    source_id = cursor.fetchone()[0]
                    source_ids.append(source_id)

    return source_ids

# Function to process and insert news details into the database
def insert_into_news_detail(*args, **kwargs):
    ti = kwargs['ti']
    articles = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    news_ids = []

    # Connect to the database
    conn = BaseHook.get_connection('rds_postgres_conn_news').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            # Process each article
            for entry_type, article_list in articles.items():
                for article in  article_list:
                    # Extract article details
                    title, summary, url, image_url = article.get('title', ''), article.get('summary', ''), article.get('url', ''), article.get('image_url', '')

                    # Insert news details into the database
                    cursor.execute("""
                        INSERT INTO dim_news_detail (title, summary, url, img_url)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (title, summary)
                        DO UPDATE SET url = EXCLUDED.url,
                                    img_url = EXCLUDED.img_url
                        RETURNING news_id;
                    """, (title, summary, url, image_url))
                    news_id = cursor.fetchone()[0]
                    news_ids.append(news_id)

    return news_ids

# Function to process and insert type data into the database
def insert_into_type(*args, **kwargs):
    ti = kwargs['ti']
    articles = ti.xcom_pull(key='json_data', task_ids='read_from_s3')
    type_ids = []

    # Connect to the database
    conn = BaseHook.get_connection('rds_postgres_conn_news').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            # Process each article
            for entry_type, article_list in articles.items():
                for article in  article_list:
                    # Insert type data into the database
                    cursor.execute("""
                        INSERT INTO dim_category (category_name)
                        VALUES (%s)
                        ON CONFLICT (category_name)
                        DO UPDATE SET category_name = EXCLUDED.category_name
                        RETURNING category_id;
                    """, (entry_type,))
                    type_id = cursor.fetchone()[0]
                    type_ids.append(type_id)

    return type_ids

# Function to process and insert fact news data into the database
def insert_into_fact_news(*args, **kwargs):
    ti = kwargs['ti']

    # Pull the necessary IDs from previous tasks
    time_ids = ti.xcom_pull(task_ids='insert_time')
    sentiment_ids = ti.xcom_pull(task_ids='insert_sentiment')
    source_ids = ti.xcom_pull(task_ids='insert_source')
    news_ids = ti.xcom_pull(task_ids='insert_news_detail')
    type_ids = ti.xcom_pull(task_ids='insert_type')

    # Connect to the database
    conn = BaseHook.get_connection('rds_postgres_conn_news').get_uri()
    with psycopg2.connect(conn) as connection:
        with connection.cursor() as cursor:
            # Combine IDs to form fact_news records
            for i in range(len(news_ids)):
                # Assuming each list has the same length and corresponding indices match
                news_id = news_ids[i]
                time_id = time_ids[i]
                sentiment_id = sentiment_ids[i]
                source_id = source_ids[i]
                type_id = type_ids[i]

                # Insert fact news data into the database
                cursor.execute("""
                    INSERT INTO fact_news (news_id, time_id, source_id, category_id, sentiment_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (news_id, time_id, source_id, type_id, sentiment_id))

            connection.commit()

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
with DAG(
        dag_id='news_airflow',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2023, 3, 1),
        catchup=False
) as dag:
    read_from_s3 = PythonOperator(
        task_id='read_from_s3',
        python_callable=read_json_from_s3,
        op_kwargs={
            'key': 'space-news-data-2023-12-20T05.json',
            'bucket_name': 'swagger23',
        }
    )

    insert_news_detail = PythonOperator(
        task_id='insert_news_detail',
        python_callable=insert_into_news_detail,
        op_kwargs={}
    )

    insert_time = PythonOperator(
        task_id='insert_time',
        python_callable=insert_into_time,
        op_kwargs={}
    )

    insert_sentiment = PythonOperator(
        task_id='insert_sentiment',
        python_callable=insert_into_sentiment,
        op_kwargs={}
    )

    insert_source = PythonOperator(
        task_id='insert_source',
        python_callable=insert_into_source,
        op_kwargs={}
    )
    insert_type = PythonOperator(
        task_id='insert_type',
        python_callable=insert_into_type,
        op_kwargs={}
    )

    insert_fact_news = PythonOperator(
        task_id='insert_fact_news',
        python_callable=insert_into_fact_news,
        op_kwargs={}
    )
    


# Setting up task dependencies
read_from_s3 >> [insert_source, insert_time, insert_news_detail, insert_sentiment, insert_type] 
[insert_source, insert_time, insert_news_detail, insert_sentiment, insert_type] >>  insert_fact_news
