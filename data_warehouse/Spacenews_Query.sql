-- Create the 'time' dimension table
CREATE TABLE time (
    time_id SERIAL PRIMARY KEY,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    hour INT,
    minute INT
);

-- Create the 'type' table
CREATE TABLE type (
    type_id SERIAL PRIMARY KEY,
    name TEXT
);

-- Create the 'sentiment' table
CREATE TABLE sentiment (
    sentiment_id SERIAL PRIMARY KEY,
    score DECIMAL
);

-- Create the 'source' table
CREATE TABLE source (
    source_id SERIAL PRIMARY KEY,
    name TEXT
);

-- Create the 'news' table
CREATE TABLE news (
    news_id SERIAL PRIMARY KEY,
    title TEXT,
    summary TEXT,
    url TEXT,
    img_url TEXT
);

-- Create the 'fact_sheet' table which references other tables
CREATE TABLE fact_sheet (
    fact_sheet_id SERIAL PRIMARY KEY,
    news_id INT REFERENCES news (news_id),
    time_id INT REFERENCES time (time_id),
    source_id INT REFERENCES source (source_id),
    type_id INT REFERENCES type (type_id),
    sentiment_id INT REFERENCES sentiment (sentiment_id)
);

-- Optional: Query to check the created tables in the current database schema
SELECT * FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';

