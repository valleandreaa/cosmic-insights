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

-- Create the 'magnitude' table
CREATE TABLE magnitude (
    magnitude_id SERIAL PRIMARY KEY,
    magnitude DECIMAL
);

-- Create the 'type' table
CREATE TABLE type (
    type_id SERIAL PRIMARY KEY,
    description TEXT,
    class_range TEXT
);

-- Create the 'diameter' table
CREATE TABLE diameter (
    diameter_id SERIAL PRIMARY KEY,
    kilometer DECIMAL,
    meter DECIMAL,
    miles DECIMAL,
    feet DECIMAL
);

-- Create the 'asteroid' table
CREATE TABLE asteroid (
    asteroid_id SERIAL PRIMARY KEY,
    name TEXT,
    is_potentially_hazardous BOOLEAN,
    is_sentry_object BOOLEAN,
    arc_in_days INT,
    observations_used INT,
    orbit_uncertainty DECIMAL,
    minimum_orbit_intersection DECIMAL,
    epoch_osculation DECIMAL,
    eccentricity DECIMAL,
    semi_major_axis DECIMAL,
    inclination DECIMAL,
    ascending_node_longitude DECIMAL,
    orbital_period DECIMAL,
    perihelion_distance DECIMAL,
    perihelion_argument DECIMAL,
    aphelion_distance DECIMAL,
    perihelion_time DECIMAL,
    mean_anomaly DECIMAL,
    mean_motion DECIMAL,
    equinox TEXT,
    orbit_determination_date TIMESTAMP
);

-- Create the 'fact_sheet' table which references other tables
CREATE TABLE fact_sheet (
    fact_sheet_id SERIAL PRIMARY KEY,
    time_id INT REFERENCES time (time_id),
    diameter_id INT REFERENCES diameter (diameter_id),
    magnitude_id INT REFERENCES magnitude (magnitude_id),
    asteroid_id INT REFERENCES asteroid (asteroid_id),
    type_id INT REFERENCES type (type_id),
    name TEXT
);

SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';

