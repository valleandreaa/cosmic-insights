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

-- Create the 'magnitude' table
CREATE TABLE dim_magnitude (
    magnitude_id SERIAL PRIMARY KEY,
    magnitude DECIMAL,
    CONSTRAINT uq_dim_magnitude_unique_values UNIQUE (magnitude)
);

-- Create the 'type' table
CREATE TABLE dim_type (
    type_id SERIAL PRIMARY KEY,
    type_name TEXT,
    description TEXT,
    class_range TEXT,
    CONSTRAINT uq_dim_type_unique_values UNIQUE (class_range)
);

-- Create the 'diameter' table
CREATE TABLE dim_diameter (
    diameter_id SERIAL PRIMARY KEY,
    kilometer DECIMAL,
    meter DECIMAL,
    miles DECIMAL,
    feet DECIMAL,
    CONSTRAINT uq_dim_diameter_unique_values UNIQUE (kilometer, meter, miles, feet)
);

-- Create the 'asteroid' table
CREATE TABLE dim_asteroid_detail (
    asteroid_id SERIAL PRIMARY KEY,
    is_potentially_hazardous BOOLEAN,
    is_sentry_object BOOLEAN,
    arc_in_days INT,
    minimum_orbit_intersection DECIMAL,
    epoch_osculation DECIMAL,
    eccentricity DECIMAL,
    semi_major_axis DECIMAL,
    inclination DECIMAL,
    ascending_node_longitude DECIMAL,
    orbital_period DECIMAL,
    perihelion_distance DECIMAL,
    perihelion_argument DECIMAL,
    mean_anomaly DECIMAL,
    mean_motion DECIMAL,
    orbit_determination_date TIMESTAMP,
    CONSTRAINT uq_dim_asteroid_unique_values UNIQUE (is_potentially_hazardous, is_sentry_object, arc_in_days, minimum_orbit_intersection, epoch_osculation, eccentricity, semi_major_axis, inclination, ascending_node_longitude, orbital_period, perihelion_distance, perihelion_argument, mean_anomaly, mean_motion, orbit_determination_date)
);

-- Create the 'fact_sheet' table which references other tables
CREATE TABLE fact_asteroid (
    fact_sheet_id SERIAL PRIMARY KEY,
    time_id INT REFERENCES dim_time (time_id),
    diameter_id INT REFERENCES dim_diameter (diameter_id),
    magnitude_id INT REFERENCES dim_magnitude (magnitude_id),
    asteroid_id INT REFERENCES dim_asteroid_detail (asteroid_id),
    type_id INT REFERENCES dim_type (type_id),
    asteroid_name TEXT,
    CONSTRAINT uq_fact_sheet_unique_values UNIQUE (time_id, diameter_id, magnitude_id, asteroid_id, type_id, asteroid_name)
);

-- Optional: Query to check the created tables in the current database schema
SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
