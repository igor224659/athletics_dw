-- Layer 2: Reconciled Data Tables

-- Reconciled Athletes (clean, standardized)
CREATE TABLE reconciled.athletes (
    athlete_id SERIAL PRIMARY KEY,
    athlete_name VARCHAR(100) NOT NULL,
    athlete_name_clean VARCHAR(100),
    nationality VARCHAR(50),
    nationality_standardized VARCHAR(50),
    gender CHAR(1),
    birth_year INT,
    age_group VARCHAR(20),
    specialization VARCHAR(30),
    data_quality_score INT,
    source_system VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reconciled Events (standardized event information)
CREATE TABLE reconciled.events (
    event_id SERIAL PRIMARY KEY,
    event_name VARCHAR(50) NOT NULL,
    event_name_standardized VARCHAR(50),
    event_category VARCHAR(20), -- Track/Field
    event_group VARCHAR(30),    -- Sprint/Distance/Jump/Throw
    distance_meters INT,
    measurement_unit VARCHAR(20),
    is_outdoor_event BOOLEAN,
    world_record_men DECIMAL(10,3),
    world_record_women DECIMAL(10,3),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reconciled Venues (geographic data integrated)
CREATE TABLE reconciled.venues (
    venue_id SERIAL PRIMARY KEY,
    venue_name VARCHAR(100) NOT NULL,
    venue_name_clean VARCHAR(100),
    city_name VARCHAR(50),
    country_name VARCHAR(50),
    country_code VARCHAR(3),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    altitude INT,
    altitude_category VARCHAR(20),
    climate_zone VARCHAR(30),
    time_zone VARCHAR(50),
    data_quality_score INT,
    geographic_source VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reconciled Performances (integrated performance data)
CREATE TABLE reconciled.performances (
    performance_id SERIAL PRIMARY KEY,
    athlete_id INT REFERENCES reconciled.athletes(athlete_id),
    event_id INT REFERENCES reconciled.events(event_id),
    venue_id INT REFERENCES reconciled.venues(venue_id),
    competition_date DATE,
    competition_name VARCHAR(100),
    competition_level VARCHAR(30),
    result_value DECIMAL(10,3),
    result_standardized DECIMAL(10,3),
    wind_reading DECIMAL(4,2),
    temperature DECIMAL(5,2),
    position_finish INT,
    is_personal_best BOOLEAN,
    is_season_best BOOLEAN,
    data_source VARCHAR(50),
    data_quality_score INT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_reconciled_perf_athlete ON reconciled.performances(athlete_id);
CREATE INDEX idx_reconciled_perf_event ON reconciled.performances(event_id);
CREATE INDEX idx_reconciled_perf_venue ON reconciled.performances(venue_id);
CREATE INDEX idx_reconciled_perf_date ON reconciled.performances(competition_date);

COMMENT ON SCHEMA reconciled IS 'Layer 2: Clean, integrated, business-ready data';