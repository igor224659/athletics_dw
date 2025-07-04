-- FACT TABLE (Performance Analysis with Environmental Context)

CREATE TABLE dwh.fact_performance (
    performance_id SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    athlete_key INT REFERENCES dwh.dim_athlete(athlete_key),
    event_key INT REFERENCES dwh.dim_event(event_key),
    venue_key INT REFERENCES dwh.dim_venue(venue_key),
    date_key INT REFERENCES dwh.dim_date(date_key),
    weather_key INT REFERENCES dwh.dim_weather(weather_key),
    -- competition_key INT REFERENCES dwh.dim_competition(competition_key),
    -- NOTE: competition_key REMOVED for simplification
    
    -- PRIMARY RESULTS
    result_value DECIMAL(10,3),  -- Time in seconds OR distance in meters
    rank_position VARCHAR(10),           -- Position in competition (1st, 2nd, etc.)
    wind_reading DECIMAL(4,2),   -- Wind speed in m/s (for applicable events)

    -- STANDARDIZE MEASURES
    performance_score DECIMAL(10,3),        -- Standardized 0-1000 score
    altitude_adjusted_result DECIMAL(10,3), -- Altitude compensation

    -- ENVIRONMENTAL IMPACT MEASURES
    temperature_impact_factor DECIMAL(5,3) DEFAULT 1.0,  -- Temperature effect factor (1.0 = neutral)
    performance_advantage DECIMAL(8,3) DEFAULT 0.0,       -- Performance vs venue average (points)
    environmental_bonus DECIMAL(8,3) DEFAULT 0.0,         -- Combined environmental benefit estimate
    
    -- PERFORMANCE CONTEXT
    --is_personal_best BOOLEAN DEFAULT FALSE,
    --is_season_best BOOLEAN DEFAULT FALSE,
    --is_championship_performance BOOLEAN DEFAULT FALSE,
    --is_world_record BOOLEAN DEFAULT FALSE,
    --is_national_record BOOLEAN DEFAULT FALSE,
    has_wind_data BOOLEAN DEFAULT FALSE,
    
    -- Data Quality
    data_quality_score INT,
    data_source VARCHAR(100),
    load_batch_id INTEGER DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- PERFORMANCE INDEXES

-- Foreign key indexes for join performance
CREATE INDEX idx_fact_athlete ON dwh.fact_performance(athlete_key);
CREATE INDEX idx_fact_event ON dwh.fact_performance(event_key);
CREATE INDEX idx_fact_venue ON dwh.fact_performance(venue_key);
CREATE INDEX idx_fact_date ON dwh.fact_performance(date_key);
CREATE INDEX idx_fact_weather ON dwh.fact_performance(weather_key);
--CREATE INDEX idx_fact_competition ON dwh.fact_performance(competition_key);

-- Measure indexes for analytical queries
CREATE INDEX idx_fact_performance_score ON dwh.fact_performance(performance_score);
CREATE INDEX idx_fact_result_value ON dwh.fact_performance(result_value);
CREATE INDEX idx_fact_rank_position ON dwh.fact_performance(rank_position);

-- Environmental analysis indexes
CREATE INDEX idx_fact_temp_impact ON dwh.fact_performance(temperature_impact_factor);
CREATE INDEX idx_fact_perf_advantage ON dwh.fact_performance(performance_advantage);
CREATE INDEX idx_fact_env_bonus ON dwh.fact_performance(environmental_bonus);


-- Performance context indexes
--CREATE INDEX idx_fact_is_championship ON dwh.fact_performance(is_championship_performance);
--CREATE INDEX idx_fact_is_pb ON dwh.fact_performance(is_personal_best);
--CREATE INDEX idx_fact_records ON dwh.fact_performance(is_world_record, is_national_record);


-- COMPOSITE INDEXES FOR COMMON BUSINESS QUESTIONS (ENVIRONMENTAL IMPACT ANALYSIS)

-- Environmental impact analysis: event + venue + weather
CREATE INDEX idx_fact_environmental_analysis ON dwh.fact_performance(event_key, venue_key, weather_key);

-- Athlete performance tracking: athlete + event + date
CREATE INDEX idx_fact_athlete_progression ON dwh.fact_performance(athlete_key, event_key, date_key);

-- Geographic performance patterns: venue + athlete
CREATE INDEX idx_fact_geographic_patterns ON dwh.fact_performance(venue_key, athlete_key);

-- Temporal performance trends: date + event
CREATE INDEX idx_fact_temporal_trends ON dwh.fact_performance(date_key, event_key);

-- Weather impact: weather + event
CREATE INDEX idx_fact_weather_impact ON dwh.fact_performance(weather_key, event_key);

-- Competition quality analysis: competition + performance flags
-- CREATE INDEX idx_fact_competition_quality ON dwh.fact_performance(competition_key, is_championship_performance);


COMMENT ON TABLE dwh.fact_performance IS 'Athletic performances with environmental context - Grain: One performance per athlete/event/venue/date/competition';


-- ========================================
-- BUSINESS QUESTIONS SUPPORTED
-- ========================================
/*
Business Grain: One performance per athlete/event/venue/date/weather combination (5 dimensions).
Focuses on environmental factors affecting athletic performance.
*/

/*
This simplified fact table supports these key environmental impact questions:

1. How do altitude and temperature affect performance by event type?
   - JOIN venue (altitude) + weather (temperature) + event (type) → performance measures

2. Which geographic locations produce the best performances?
   - JOIN venue (geography) + athlete (nationality) → performance measures

3. How has performance evolved over time and seasons?
   - JOIN date (time periods) + event → performance measures

4. How do environmental conditions affect different athlete specializations?
   - JOIN athlete (specialization) + weather + venue → performance measures

5. What is the performance advantage of different venues and conditions?
   - Use performance_advantage and environmental_bonus measures

All questions focus on environmental impact without competition complexity.
*/
