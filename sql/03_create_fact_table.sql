-- FACT TABLE (Performance Analysis with Environmental Context)

CREATE TABLE dwh.fact_performance (
    performance_id SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    athlete_key INT REFERENCES dwh.dim_athlete(athlete_key),
    event_key INT REFERENCES dwh.dim_event(event_key),
    venue_key INT REFERENCES dwh.dim_venue(venue_key),
    date_key INT REFERENCES dwh.dim_date(date_key),
    competition_key INT REFERENCES dwh.dim_competition(competition_key),
    weather_key INT REFERENCES dwh.dim_weather(weather_key), -- NEW
    
    -- Performance Measures
    result_value DECIMAL(10,3),
    performance_score DECIMAL(10,3), -- Standardized 0-1000 score
    altitude_adjusted_result DECIMAL(10,3), -- Environmental adjustment
    rank_position INT,
    wind_reading DECIMAL(4,2),
    
    -- Performance Context
    is_personal_best BOOLEAN DEFAULT FALSE,
    is_season_best BOOLEAN DEFAULT FALSE,
    is_championship_performance BOOLEAN DEFAULT FALSE,
    
    -- Environmental Impact Measures
    temperature_impact_factor DECIMAL(5,3), -- Estimated temperature effect
    performance_advantage DECIMAL(8,3),     -- vs venue average
    environmental_bonus DECIMAL(8,3),       -- estimated improvement due to conditions
    
    -- Data Quality
    data_quality_score INT,
    source_system VARCHAR(50),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact table indexes
CREATE INDEX idx_fact_athlete ON dwh.fact_performance(athlete_key);
CREATE INDEX idx_fact_event ON dwh.fact_performance(event_key);
CREATE INDEX idx_fact_venue ON dwh.fact_performance(venue_key);
CREATE INDEX idx_fact_date ON dwh.fact_performance(date_key);
CREATE INDEX idx_fact_competition ON dwh.fact_performance(competition_key);
CREATE INDEX idx_fact_weather ON dwh.fact_performance(weather_key);
CREATE INDEX idx_fact_performance_score ON dwh.fact_performance(performance_score);
CREATE INDEX idx_fact_is_championship ON dwh.fact_performance(is_championship_performance);

-- Composite indexes for common queries
CREATE INDEX idx_fact_event_venue ON dwh.fact_performance(event_key, venue_key);
CREATE INDEX idx_fact_athlete_event ON dwh.fact_performance(athlete_key, event_key);

COMMENT ON TABLE dwh.fact_performance IS 'Athletic performances with environmental context - Grain: One performance per athlete/event/venue/date/competition';