-- Create the 'time' dimension table
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    hour INT,
    minute INT,
    CONSTRAINT uq_dim_time_unique_values UNIQUE (year, quarter, month, week, day, hour, minute)
);

-- Create the 'type' table
CREATE TABLE dim_type (
    type_id SERIAL PRIMARY KEY,
    type_name TEXT,
    CONSTRAINT uq_dim_type_unique_values UNIQUE (type_name)
);

-- Create the 'sentiment' table
CREATE TABLE dim_sentiment (
    sentiment_id SERIAL PRIMARY KEY,
    score DECIMAL,
    CONSTRAINT uq_dim_sentiment_unique_values UNIQUE (score)
);

-- Create the 'source' table
CREATE TABLE dim_source (
    source_id SERIAL PRIMARY KEY,
    source_name TEXT,
    CONSTRAINT uq_dim_source_unique_values UNIQUE (source_name)
);

-- Create the 'news' table
CREATE TABLE dim_news_detail (
    news_id SERIAL PRIMARY KEY,
    title TEXT,
    summary TEXT,
    url TEXT,
    img_url TEXT,
    CONSTRAINT uq_dim_news_detail_unique_values UNIQUE (title, summary)
);

-- Create the 'fact_news' table which references other tables
CREATE TABLE fact_news (
    fact_sheet_id SERIAL PRIMARY KEY,
    news_id INT REFERENCES dim_news_detail (news_id),
    time_id INT REFERENCES dim_time (time_id),
    source_id INT REFERENCES dim_source (source_id),
    type_id INT REFERENCES dim_type (type_id),
    sentiment_id INT REFERENCES dim_sentiment (sentiment_id),
    CONSTRAINT uq_fact_sheet_unique_values UNIQUE (news_id, time_id, source_id, type_id)
);

-- Optional: Query to check the created tables in the current database schema
SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
