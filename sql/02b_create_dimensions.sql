-- ===========================
-- DIMENSION TABLES (Based on Original + Environmental Conditions)
-- ===========================

-- Time Dimension
CREATE TABLE dwh.dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE,
    year INT,
    season VARCHAR(20), -- 'Indoor', 'Outdoor'
    competition_level VARCHAR(30), -- 'Elite', 'Professional', 'Amateur'
    is_championship_year BOOLEAN,
    decade VARCHAR(10), -- '2000s', '2010s', '2020s'
    month_name VARCHAR(20),
    quarter INT
);

-- Geographic Dimension  
CREATE TABLE dwh.dim_venue (
    venue_key SERIAL PRIMARY KEY,
    venue_name VARCHAR(100),
    city_name VARCHAR(50),
    country_name VARCHAR(50),
    country_code VARCHAR(3),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    altitude INT,
    altitude_category VARCHAR(20), -- 'Sea Level', 'Moderate', 'High'
    climate_zone VARCHAR(50)
);

-- Event Dimension
CREATE TABLE dwh.dim_event (
    event_key SERIAL PRIMARY KEY,
    event_name VARCHAR(50),
    event_category VARCHAR(20), -- 'Track', 'Field' 
    event_group VARCHAR(30), -- 'Sprint', 'Distance', 'Jumps', 'Throws'
    distance_meters INT,
    measurement_unit VARCHAR(20), -- 'seconds', 'meters'
    gender VARCHAR(10), -- 'Men', 'Women'
    is_outdoor_event BOOLEAN,
    world_record DECIMAL(10,3)
);

-- Athlete Dimension
CREATE TABLE dwh.dim_athlete (
    athlete_key SERIAL PRIMARY KEY,
    athlete_name VARCHAR(100),
    nationality VARCHAR(50),
    nationality_code VARCHAR(3),
    gender CHAR(1),
    birth_decade VARCHAR(10), -- '1980s', '1990s'
    specialization VARCHAR(30) -- 'Sprinter', 'Distance', 'Jumper', 'Thrower'
);


-- Weather Dimension
CREATE TABLE dwh.dim_weather (
    weather_key SERIAL PRIMARY KEY,
    venue_name VARCHAR(50),
    month_name VARCHAR(20),
    temperature DECIMAL(5,2),
    temperature_category VARCHAR(20), -- 'Cold', 'Cool', 'Moderate', 'Warm', 'Hot'
    season_category VARCHAR(20), -- 'Winter', 'Spring', 'Summer', 'Fall'
    has_actual_data BOOLEAN -- TRUE if real weather data, FALSE if estimated
);

-- Create indexes for better performance
CREATE INDEX idx_venue_city ON dwh.dim_venue(city_name);
CREATE INDEX idx_venue_country ON dwh.dim_venue(country_name);
CREATE INDEX idx_venue_altitude ON dwh.dim_venue(altitude_category);
CREATE INDEX idx_event_name ON dwh.dim_event(event_name);
CREATE INDEX idx_event_group ON dwh.dim_event(event_group);
CREATE INDEX idx_athlete_name ON dwh.dim_athlete(athlete_name);
CREATE INDEX idx_athlete_nationality ON dwh.dim_athlete(nationality);
CREATE INDEX idx_date_year ON dwh.dim_date(year);
CREATE INDEX idx_date_season ON dwh.dim_date(season);
--CREATE INDEX idx_competition_level ON dwh.dim_competition(competition_level);
CREATE INDEX idx_weather_temp_category ON dwh.dim_weather(temperature_category);

COMMENT ON SCHEMA dwh IS 'Data Warehouse schema for athletics performance analysis';
COMMENT ON TABLE dwh.dim_weather IS 'Environmental conditions dimension for climate impact analysis';