-- ===========================
-- FACT TABLE
-- ===========================

CREATE TABLE dwh.fact_performance (
    performance_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    athlete_key INT REFERENCES dwh.dim_athlete(athlete_key),
    event_key INT REFERENCES dwh.dim_event(event_key),
    venue_key INT REFERENCES dwh.dim_venue(venue_key),
    date_key INT REFERENCES dwh.dim_date(date_key),
    competition_key INT REFERENCES dwh.dim_competition(competition_key),
    weather_key INT REFERENCES dwh.dim_weather(weather_key),
    
    -- Measures
    result_value DECIMAL(10,3), -- time in seconds or distance in meters
    wind_reading DECIMAL(4,2), -- wind speed if available
    position_finish INT, -- finishing position
    performance_score DECIMAL(8,2), -- standardized performance score (0-1000)
    altitude_adjusted_result DECIMAL(10,3), -- performance adjusted for altitude
    
    -- Performance Indicators
    is_championship_performance BOOLEAN,
    is_personal_best BOOLEAN,
    is_season_best BOOLEAN,
    is_world_record BOOLEAN,
    is_national_record BOOLEAN,
    
    -- Data Quality
    data_source VARCHAR(30), -- 'WorldAthletics'
    data_quality_score INT, -- 1-10 reliability score
    has_wind_data BOOLEAN,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    load_batch_id INT
);

-- Performance Indexes for OLAP queries
CREATE INDEX idx_fact_athlete_event ON dwh.fact_performance(athlete_key, event_key);
CREATE INDEX idx_fact_venue_date ON dwh.fact_performance(venue_key, date_key);
CREATE INDEX idx_fact_performance_score ON dwh.fact_performance(performance_score);
CREATE INDEX idx_fact_competition_level ON dwh.fact_performance(competition_key);
CREATE INDEX idx_fact_result_value ON dwh.fact_performance(result_value);

-- Composite indexes for common queries
CREATE INDEX idx_fact_env_analysis ON dwh.fact_performance(venue_key, weather_key, event_key);
CREATE INDEX idx_fact_time_analysis ON dwh.fact_performance(date_key, competition_key);

COMMENT ON TABLE dwh.fact_performance IS 'Central fact table containing athletic performance measurements';