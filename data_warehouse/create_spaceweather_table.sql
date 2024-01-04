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

-- Create the 'plasma' table
CREATE TABLE dim_plasma (
    plasma_id SERIAL PRIMARY KEY,
    density DECIMAL,
    speed DECIMAL,
    CONSTRAINT uq_dim_plasma_unique_values UNIQUE (density, speed)
);

-- Create the 'magnetometer' table
CREATE TABLE dim_magnetometer (
    magnetometer_id SERIAL PRIMARY KEY,
    he DECIMAL,
    hp DECIMAL,
    hn DECIMAL,
    total DECIMAL,
    CONSTRAINT uq_dim_magnetometer_unique_values UNIQUE (he, hp, hn, total)
);

-- Create the 'mag' table
CREATE TABLE dim_mag (
    mag_id SERIAL PRIMARY KEY,
    bx_gsm DECIMAL,
    by_gsm DECIMAL,
    bz_gsm DECIMAL,
    CONSTRAINT uq_dim_mag_unique_values UNIQUE (bx_gsm, by_gsm, bz_gsm)
);

-- Create the 'proton' table
CREATE TABLE dim_proton (
    proton_id SERIAL PRIMARY KEY,
    flux DECIMAL,
    energy DECIMAL,
    CONSTRAINT uq_dim_proton_unique_values UNIQUE (flux, energy)
);

-- Create the 'fact_sheet' table which references other tables
CREATE TABLE fact_sheet (
    fact_sheet_id SERIAL PRIMARY KEY,
    weather_id INT,
    plasma_id INT REFERENCES dim_plasma (plasma_id),
    time_id INT REFERENCES dim_time (time_id),
    magnetometer_id INT REFERENCES dim_magnetometer (magnetometer_id),
    mag_id INT REFERENCES dim_mag (mag_id),
    proton_id INT REFERENCES dim_proton (proton_id),
    CONSTRAINT uq_fact_sheet_unique_values UNIQUE (weather_id, plasma_id, time_id, magnetometer_id, mag_id, proton_id)
);

-- Optional: Query to check the created tables in the current database schema
SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
