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

-- Create the 'plasma' table
CREATE TABLE plasma (
    plasma_id SERIAL PRIMARY KEY,
    density DECIMAL,
    speed DECIMAL
);

-- Create the 'magnetometer' table
CREATE TABLE magnetometer (
    magnetometer_id SERIAL PRIMARY KEY,
    he DECIMAL,
    hp DECIMAL,
    hn DECIMAL,
    total DECIMAL
);

-- Create the 'mag' table
CREATE TABLE mag (
    mag_id SERIAL PRIMARY KEY,
    bx_gsm DECIMAL,
    by_gsm DECIMAL,
    bz_gsm DECIMAL
);

-- Create the 'proton' table
CREATE TABLE proton (
    proton_id SERIAL PRIMARY KEY,
    flux DECIMAL,
    energy DECIMAL
);

-- Create the 'fact_sheet' table which references other tables
CREATE TABLE fact_sheet (
    fact_sheet_id SERIAL PRIMARY KEY,
    weather_id INT,
    plasma_id INT REFERENCES plasma (plasma_id),
    time_id INT REFERENCES time (time_id),
    magnetometer_id INT REFERENCES magnetometer (magnetometer_id),
    mag_id INT REFERENCES mag (mag_id),
    proton_id INT REFERENCES proton (proton_id)
);

-- Optional: Query to check the created tables in the current database schema
SELECT * FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';

