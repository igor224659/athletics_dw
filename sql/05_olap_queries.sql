-- REFINED OLAP ANALYSIS QUERIES
-- Environmental Impact Analysis on Athletic Performance

-- -- ===========================
-- -- 1. 3-LAYER ARCHITECTURE VERIFICATION
-- -- ===========================

-- -- Verify data flow across all three layers
-- SELECT 
--     'Layer 1 (Staging)' as layer,
--     COUNT(*) as total_records,
--     COUNT(DISTINCT athlete_name) as unique_athletes,
--     COUNT(DISTINCT event_clean) as unique_events
-- FROM staging.clean_world_athletics
-- UNION ALL
-- SELECT 'Layer 2 (Reconciled)',
--     COUNT(*), COUNT(DISTINCT athlete_key), COUNT(DISTINCT event_key)
-- FROM reconciled.performances
-- UNION ALL
-- SELECT 'Layer 3 (Star Schema)',
--     COUNT(*), COUNT(DISTINCT athlete_key), COUNT(DISTINCT event_key)
-- FROM dwh.fact_performance
-- ORDER BY layer;


-- ===========================
-- 2. ENVIRONMENTAL IMPACT ANALYSIS
-- ===========================
-- How do altitude and temperature affect performance? SLICE AND DICE

-- Slice: Filters data by specific conditions (altitude_category IS NOT NULL)
-- Dice: Creates a 3-dimensional cube view with:

-- Dimension 1: Altitude categories (Sea Level, Moderate, High)
-- Dimension 2: Temperature categories (Cold, Cool, Moderate, Warm, Hot)
-- Dimension 3: Event categories (Track, Field)

-- Business Value: Cross-sectional analysis of environmental factors

SELECT 
    v.altitude_category,
    w.temperature_category,
    e.event_category,
    COUNT(*) as performances,
    ROUND(AVG(f.performance_score), 1) as avg_performance_score,
    ROUND(AVG(f.temperature_impact_factor), 3) as avg_temp_impact,
    ROUND(AVG(f.altitude_adjusted_result - f.result_value), 3) as avg_altitude_adjustment
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
JOIN dwh.dim_weather w ON f.weather_key = w.weather_key
JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE v.altitude_category IS NOT NULL 
  AND w.temperature_category IS NOT NULL
GROUP BY v.altitude_category, w.temperature_category, e.event_category
HAVING COUNT(*) >= 20  -- Minimum sample size
ORDER BY avg_performance_score DESC
LIMIT 15;


-- ===========================
-- 3. GEOGRAPHIC PERFORMANCE PATTERNS
-- ===========================
-- Which countries excel in athletics?

-- Roll-up: Aggregates from venue level → city level → country level
-- Hierarchy: Venue → City → Country → Continent (stopping at country)

-- Aggregated Measures:
-- Total performances per country
-- Average performance scores per country
-- Athlete counts per country

-- Business Value: High-level geographic summary for strategic analysis

SELECT 
    v.country_code,
    COUNT(*) as total_performances,
    COUNT(DISTINCT f.athlete_key) as unique_athletes,
    COUNT(DISTINCT f.event_key) as events_covered,
    ROUND(AVG(f.performance_score), 1) as avg_performance_score,
    COUNT(CASE WHEN f.performance_score > 1000 THEN 1 END) as elite_performances,
    ROUND(AVG(v.altitude), 0) as avg_altitude
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
WHERE v.country_code IS NOT NULL AND v.country_code != 'Unknown'
GROUP BY v.country_code
HAVING COUNT(*) >= 100  -- Countries with significant data
ORDER BY avg_performance_score DESC
LIMIT 20;


-- ===========================
-- 4. TEMPORAL PERFORMANCE TRENDS
-- ===========================
-- Performance evolution over time and seasons

--OLAP Operation: DRILL-DOWN
--Drill-down: Breaks down time dimension into multiple levels:
--Level 1: Decade (2010s, 2020s)
--Level 2: Season (Indoor, Outdoor)
--Level 3: Event Category (Track, Field)

-- Time Hierarchy: All Time → Decade → Year → Season → Month
-- Business Value: Detailed temporal analysis showing trends within trends

SELECT 
    d.decade,
    d.season,
    e.event_category,
    COUNT(*) as performances,
    ROUND(AVG(f.performance_score), 1) as avg_score,
    ROUND(AVG(f.temperature_impact_factor), 3) as avg_temp_impact,
    COUNT(CASE WHEN f.performance_score > 1000 THEN 1 END) as elite_count
FROM dwh.fact_performance f
JOIN dwh.dim_date d ON f.date_key = d.date_key
JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE d.year >= 2010
GROUP BY d.decade, d.season, e.event_category
HAVING COUNT(*) >= 50
ORDER BY d.decade, e.event_category, avg_score DESC;


-- ===========================
-- 5. VENUE SPECIALIZATION ANALYSIS (PIVOTING)
-- ===========================
-- Which venues are best for different event types?

-- Pivot: Rotates event_group dimension from rows to columns
-- Before Pivot: Rows would be (Venue, Event_Group) pairs
-- After Pivot: Columns show Sprint_Avg, Distance_Avg, Jumps_Avg, Throws_Avg
-- Matrix View: Venues vs Event Types performance matrix
-- Business Value: Easy comparison of venue strengths across event types

SELECT 
    v.venue_name,
    v.country_code,
    v.altitude_category,
    COUNT(*) as total_performances,
    -- Pivot by event group
    ROUND(AVG(CASE WHEN e.event_group = 'Sprint' THEN f.performance_score END), 1) as sprint_avg,
    ROUND(AVG(CASE WHEN e.event_group = 'Distance' THEN f.performance_score END), 1) as distance_avg,
    ROUND(AVG(CASE WHEN e.event_group = 'Jumps' THEN f.performance_score END), 1) as jumps_avg,
    ROUND(AVG(CASE WHEN e.event_group = 'Throws' THEN f.performance_score END), 1) as throws_avg,
    -- Overall venue quality
    ROUND(AVG(f.performance_score), 1) as overall_avg
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE v.venue_name != 'Unknown'
GROUP BY v.venue_name, v.country_code, v.altitude_category
HAVING COUNT(*) >= 50  -- Venues with significant activity
  AND COUNT(DISTINCT e.event_group) >= 2  -- Multi-event venues
ORDER BY overall_avg DESC
LIMIT 15;


-- ===========================
-- 6. ATHLETE PERFORMANCE RANKING
-- ===========================
-- Top performing athletes with environmental context

-- OLAP Operation: DRILL-DOWN + SLICE

-- Drill-down: From event category → specific event name → individual athletes
-- Slice: Filters to elite performances only (performance_score > 1000)
-- Analytical Function: Ranking within each event partition
-- Hierarchy: All Athletes → Event Category → Event Name → Individual Performance
-- Business Value: Detailed athlete analysis with contextual environmental data

WITH top_performers AS (
    SELECT 
        a.athlete_name,
        a.nationality_code,
        e.event_name,
        f.performance_score,
        v.venue_name,
        v.altitude,
        w.temperature,
        d.year,
        ROW_NUMBER() OVER (PARTITION BY e.event_name ORDER BY f.performance_score DESC) as rank_in_event
    FROM dwh.fact_performance f
    JOIN dwh.dim_athlete a ON f.athlete_key = a.athlete_key
    JOIN dwh.dim_event e ON f.event_key = e.event_key
    JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
    JOIN dwh.dim_weather w ON f.weather_key = w.weather_key
    JOIN dwh.dim_date d ON f.date_key = d.date_key
    WHERE f.performance_score > 1000
)
SELECT 
    event_name,
    athlete_name,
    nationality_code,
    performance_score,
    venue_name,
    altitude,
    temperature,
    year,
    rank_in_event
FROM top_performers
WHERE rank_in_event <= 3  -- Top 3 per event
ORDER BY event_name, rank_in_event;


-- ===========================
-- 7. ENVIRONMENTAL OPTIMIZATION INSIGHTS
-- ===========================
-- Best environmental conditions for each event type

-- OLAP Operation: DRILL-ACROSS

-- Drill-across: Analyzes the same measures (performance_score) across different dimensional perspectives:

-- Analysis 1: Event Group × Altitude Category
-- Analysis 2: Event Group × Temperature Category


-- Cross-dimensional: Compares environmental impacts across multiple condition types
-- Unified Result: Single result set showing optimal conditions from different angles
-- Business Value: Comprehensive environmental optimization recommendations

SELECT 
    e.event_group,
    'Optimal Altitude' as condition_type,
    v.altitude_category as optimal_value,
    COUNT(*) as sample_size,
    ROUND(AVG(f.performance_score), 1) as avg_performance
FROM dwh.fact_performance f
JOIN dwh.dim_event e ON f.event_key = e.event_key
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
WHERE v.altitude_category IS NOT NULL
GROUP BY e.event_group, v.altitude_category
HAVING COUNT(*) >= 100

UNION ALL

SELECT 
    e.event_group,
    'Optimal Temperature',
    w.temperature_category,
    COUNT(*),
    ROUND(AVG(f.performance_score), 1)
FROM dwh.fact_performance f
JOIN dwh.dim_event e ON f.event_key = e.event_key
JOIN dwh.dim_weather w ON f.weather_key = w.weather_key
WHERE w.temperature_category IS NOT NULL
GROUP BY e.event_group, w.temperature_category
HAVING COUNT(*) >= 100

ORDER BY event_group, avg_performance DESC;


-- -- ===========================
-- -- 8. BUSINESS INTELLIGENCE SUMMARY
-- -- ===========================

-- -- Key performance indicators for the data warehouse
-- SELECT 
--     'ANALYTICS SUMMARY' as report_section,
--     metric_name,
--     metric_value,
--     insight
-- FROM (
--     SELECT 
--         'Total Elite Performances' as metric_name,
--         COUNT(*)::VARCHAR as metric_value,
--         'Performances scoring > 1000 points' as insight
--     FROM dwh.fact_performance 
--     WHERE performance_score > 1000
    
--     UNION ALL
    
--     SELECT 
--         'Best Altitude for Sprints',
--         v.altitude_category,
--         'Optimal altitude category for sprint events'
--     FROM dwh.fact_performance f
--     JOIN dwh.dim_event e ON f.event_key = e.event_key
--     JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
--     WHERE e.event_group = 'Sprint'
--     GROUP BY v.altitude_category
--     ORDER BY AVG(f.performance_score) DESC
--     LIMIT 1
    
--     UNION ALL
    
--     SELECT 
--         'Best Temperature Range',
--         w.temperature_category,
--         'Optimal temperature for overall performance'
--     FROM dwh.fact_performance f
--     JOIN dwh.dim_weather w ON f.weather_key = w.weather_key
--     GROUP BY w.temperature_category
--     ORDER BY AVG(f.performance_score) DESC
--     LIMIT 1
    
--     UNION ALL
    
--     SELECT 
--         'Coverage Countries',
--         COUNT(DISTINCT country_name)::VARCHAR,
--         'Number of countries with venue data'
--     FROM dwh.dim_venue
--     WHERE country_name != 'Unknown'
    
-- ) summary_metrics
-- ORDER BY metric_name;