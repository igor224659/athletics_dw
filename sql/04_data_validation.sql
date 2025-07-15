-- REFINED DATA VALIDATION QUERIES
-- Concise validation checks for 3-layer athletics data warehouse

-- ===========================
-- 1. DATA COMPLETENESS CHECK
-- ===========================

-- Count performances records in all tables across 3 layers
-- SELECT 'STAGING' as layer, 'raw_world_athletics' as table_name, COUNT(*) as records
-- FROM staging.raw_world_athletics
-- UNION ALL
-- SELECT 'RECONCILED', 'performances', COUNT(*) FROM reconciled.performances
-- UNION ALL
-- SELECT 'STAR SCHEMA', 'fact_performance', COUNT(*) FROM dwh.fact_performance

-- ORDER BY records;

-- Verify data flow across all three layers
SELECT 
    'Layer 1 (Staging)' as layer,
    COUNT(*) as total_records,
    COUNT(DISTINCT athlete_name) as unique_athletes,
    COUNT(DISTINCT event_clean) as unique_events
FROM staging.clean_world_athletics
UNION ALL
SELECT 'Layer 2 (Reconciled)',
    COUNT(*), COUNT(DISTINCT athlete_key), COUNT(DISTINCT event_key)
FROM reconciled.performances
UNION ALL
SELECT 'Layer 3 (Star Schema)',
    COUNT(*), COUNT(DISTINCT athlete_key), COUNT(DISTINCT event_key)
FROM dwh.fact_performance
ORDER BY layer;


-- ===========================
-- 2. DATA QUALITY VALIDATION
-- ===========================

-- Check foreign key integrity in fact table
SELECT 
    'Missing Athletes' as check_type,
    COUNT(*) as failed_records,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM dwh.fact_performance f 
LEFT JOIN dwh.dim_athlete a ON f.athlete_key = a.athlete_key
WHERE a.athlete_key IS NULL

UNION ALL

SELECT 'Missing Events', COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dwh.fact_performance f 
LEFT JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE e.event_key IS NULL

UNION ALL

SELECT 'Missing Venues', COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dwh.fact_performance f 
LEFT JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
WHERE v.venue_key IS NULL

ORDER BY check_type;


-- ===========================
-- 3. PERFORMANCE DATA QUALITY
-- ===========================

-- Validate performance scores and key measures
SELECT 
    'Valid Performance Scores' as metric,
    COUNT(CASE WHEN performance_score BETWEEN 200 AND 1400 THEN 1 END) as valid_count,
    COUNT(*) as total_count,
    ROUND(100.0 * COUNT(CASE WHEN performance_score BETWEEN 200 AND 1400 THEN 1 END) / COUNT(*), 1) as success_rate
FROM dwh.fact_performance

UNION ALL

SELECT 'Valid Result Values',
    COUNT(CASE WHEN result_value > 0 AND result_value < 50000 THEN 1 END),
    COUNT(*),
    ROUND(100.0 * COUNT(CASE WHEN result_value > 0 AND result_value < 50000 THEN 1 END) / COUNT(*), 1)
FROM dwh.fact_performance

UNION ALL

SELECT 'Environmental Data Complete',
    COUNT(CASE WHEN temperature_impact_factor IS NOT NULL AND altitude_adjusted_result IS NOT NULL THEN 1 END),
    COUNT(*),
    ROUND(100.0 * COUNT(CASE WHEN temperature_impact_factor IS NOT NULL AND altitude_adjusted_result IS NOT NULL THEN 1 END) / COUNT(*), 1)
FROM dwh.fact_performance;


-- ===========================
-- 4. SAMPLE DATA VERIFICATION
-- ===========================

-- Show sample of integrated data to verify joins worked correctly
SELECT 
    a.athlete_name,
    a.nationality_code,
    e.event_name,
    v.venue_name,
    v.country_name as venue_country,
    v.altitude,
    w.temperature,
    f.result_value,
    f.performance_score,
    d.year
FROM dwh.fact_performance f
JOIN dwh.dim_athlete a ON f.athlete_key = a.athlete_key
JOIN dwh.dim_event e ON f.event_key = e.event_key
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
JOIN dwh.dim_weather w ON f.weather_key = w.weather_key
JOIN dwh.dim_date d ON f.date_key = d.date_key
WHERE f.performance_score > 1000 AND f.performance_score < 1350
ORDER BY f.performance_score DESC
LIMIT 10;

-- ===========================
-- 5. BUSINESS LOGIC VALIDATION
-- ===========================

-- Verify event categorization and measurement units
SELECT 
    e.event_category,
    e.measurement_unit,
    COUNT(*) as performances,
    ROUND(MIN(f.performance_score), 1) as min_score,
    ROUND(AVG(f.performance_score), 1) as avg_score,
    ROUND(MAX(f.performance_score), 1) as max_score,
    ROUND(STDDEV(f.performance_score), 1) as score_variance
FROM dwh.fact_performance f
JOIN dwh.dim_event e ON f.event_key = e.event_key
GROUP BY e.event_category, e.measurement_unit
ORDER BY e.event_category, performances DESC;


-- ===========================
-- 6. FINAL VALIDATION SUMMARY
-- ===========================

-- Overall data warehouse health check
-- SELECT 
--     'DATA WAREHOUSE HEALTH CHECK' as summary_type,
--     'Total Performances' as metric,
--     COUNT(*)::VARCHAR as value,
--     CASE WHEN COUNT(*) > 50000 THEN 'EXCELLENT' ELSE 'GOOD' END as assessment
-- FROM dwh.fact_performance

-- UNION ALL

-- SELECT 'COVERAGE', 'Unique Athletes', 
--     COUNT(DISTINCT athlete_key)::VARCHAR,
--     CASE WHEN COUNT(DISTINCT athlete_key) > 5000 THEN 'EXCELLENT' ELSE 'GOOD' END
-- FROM dwh.fact_performance

-- UNION ALL

-- SELECT 'COVERAGE', 'Unique Venues',
--     COUNT(DISTINCT venue_key)::VARCHAR,
--     CASE WHEN COUNT(DISTINCT venue_key) > 500 THEN 'EXCELLENT' ELSE 'GOOD' END
-- FROM dwh.fact_performance

-- UNION ALL

-- SELECT 'QUALITY', 'Data Quality Score',
--     ROUND(AVG(data_quality_score), 1)::VARCHAR,
--     CASE WHEN AVG(data_quality_score) >= 8 THEN 'EXCELLENT' ELSE 'NEEDS IMPROVEMENT' END
-- FROM dwh.fact_performance

-- ORDER BY summary_type, metric;