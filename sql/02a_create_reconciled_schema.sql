-- Layer 2: Reconciled Data Tables (Clean, Integrated Business Data)

-- Reconciled Athletes (clean, standardized)
CREATE TABLE reconciled.athletes (
    athlete_key SERIAL PRIMARY KEY,
    athlete_name VARCHAR(100) NOT NULL,
    athlete_name_clean VARCHAR(100),
    nationality VARCHAR(50),
    nationality_standardized VARCHAR(50),
    nationality_code VARCHAR(3),
    gender CHAR(1),
    birth_decade VARCHAR(10),
    specialization VARCHAR(30),
    data_quality_score INT,
    source_system VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reconciled Events (standardized event information)
CREATE TABLE reconciled.events (
    event_key SERIAL PRIMARY KEY,
    event_name VARCHAR(50) NOT NULL,
    event_name_standardized VARCHAR(50),
    event_category VARCHAR(20), -- Track/Field
    event_group VARCHAR(30),    -- Sprint/Distance/Jump/Throw
    distance_meters INT,
    measurement_unit VARCHAR(20),
    gender VARCHAR(10), -- Men/Women
    is_outdoor_event BOOLEAN,
    world_record DECIMAL(10,3),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reconciled Venues (geographic data integrated)
CREATE TABLE reconciled.venues (
    venue_key SERIAL PRIMARY KEY,
    venue_name VARCHAR(100) NOT NULL,
    venue_name_clean VARCHAR(100),
    city_name VARCHAR(50),
    country_name VARCHAR(50),
    country_code VARCHAR(20),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    altitude INT,
    altitude_category VARCHAR(20),
    continent VARCHAR(30),
    climate_zone VARCHAR(30),
    population INT,
    data_quality_score INT,
    geographic_source VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reconciled Weather (environmental conditions)
CREATE TABLE reconciled.weather_conditions (
    weather_key SERIAL PRIMARY KEY,
    venue_name VARCHAR(100),
    month_name VARCHAR(20),
    temperature DECIMAL(5,2),
    temperature_category VARCHAR(20), -- Cold, Cool, Moderate, Warm, Hot
    season_category VARCHAR(20),      -- Winter, Spring, Summer, Fall
    has_actual_data BOOLEAN,          -- TRUE if real data, FALSE if estimated
    weather_source VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Reconciled Performances (integrated performance data)
CREATE TABLE reconciled.performances (
    performance_key SERIAL PRIMARY KEY,
    athlete_key INT REFERENCES reconciled.athletes(athlete_key),
    event_key INT REFERENCES reconciled.events(event_key),
    venue_key INT REFERENCES reconciled.venues(venue_key),
    weather_key INT REFERENCES reconciled.weather_conditions(weather_key),
    competition_date DATE,
    result_value DECIMAL(10,3),
    wind_reading DECIMAL(4,2),
    position_finish VARCHAR(50),
    data_source VARCHAR(50),
    data_quality_score INT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_reconciled_perf_athlete ON reconciled.performances(athlete_key);
CREATE INDEX idx_reconciled_perf_event ON reconciled.performances(event_key);
CREATE INDEX idx_reconciled_perf_venue ON reconciled.performances(venue_key);
CREATE INDEX idx_reconciled_perf_date ON reconciled.performances(competition_date);

COMMENT ON SCHEMA reconciled IS 'Layer 2: Clean, integrated, business-ready data';