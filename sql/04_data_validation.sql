-- Data Validation Queries


-- DIMENSION TABLE VALIDATION

-- Check all dimension table counts
SELECT 'dim_date' as table_name, COUNT(*) as record_count FROM dwh.dim_date
UNION ALL
SELECT 'dim_venue', COUNT(*) FROM dwh.dim_venue
UNION ALL  
SELECT 'dim_event', COUNT(*) FROM dwh.dim_event
UNION ALL
SELECT 'dim_athlete', COUNT(*) FROM dwh.dim_athlete
--UNION ALL
--SELECT 'dim_competition', COUNT(*) FROM dwh.dim_competition
UNION ALL
SELECT 'dim_weather', COUNT(*) FROM dwh.dim_weather
UNION ALL
SELECT 'fact_performance', COUNT(*) FROM dwh.fact_performance
ORDER BY table_name;



-- DATA QUALITY CHECKS

-- Check for missing key relationships
SELECT 'Missing athlete keys' as issue, COUNT(*) as count, 
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM dwh.fact_performance f 
LEFT JOIN dwh.dim_athlete a ON f.athlete_key = a.athlete_key
WHERE a.athlete_key IS NULL

UNION ALL

SELECT 'Missing event keys', COUNT(*), 
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dwh.fact_performance f 
LEFT JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE e.event_key IS NULL

UNION ALL

SELECT 'Missing venue keys', COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dwh.fact_performance f 
LEFT JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
WHERE v.venue_key IS NULL

UNION ALL

SELECT 'Missing date keys', COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dwh.fact_performance f 
LEFT JOIN dwh.dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL

UNION ALL

SELECT 'Missing weather keys', COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dwh.fact_performance f 
LEFT JOIN dwh.dim_weather w ON f.weather_key = w.weather_key
WHERE w.weather_key IS NULL;

-- Check result value ranges
SELECT 
    e.event_name,
    e.measurement_unit,
    COUNT(*) as performance_count,
    MIN(f.result_value) as min_result,
    AVG(f.result_value) as avg_result,
    MAX(f.result_value) as max_result,
    STDDEV(f.result_value) as stddev_result
FROM dwh.fact_performance f
JOIN dwh.dim_event e ON f.event_key = e.event_key
GROUP BY e.event_name, e.measurement_unit
ORDER BY e.event_name;

-- Check geographic distribution
SELECT 
    v.country_name,
    COUNT(*) as performance_count,
    AVG(f.performance_score) as avg_score
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
GROUP BY v.country_name
HAVING COUNT(*) >= 5
ORDER BY avg_score DESC;

-- Check temporal distribution
SELECT 
    d.year,
    d.season,
    COUNT(*) as performance_count
FROM dwh.fact_performance f
JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.season
ORDER BY d.year, d.season;

SELECT 
    'Performance Score Distribution' as check_type,
    COUNT(CASE WHEN performance_score BETWEEN 200 AND 1200 THEN 1 END) as valid_count,
    COUNT(CASE WHEN performance_score < 200 OR performance_score > 1200 THEN 1 END) as invalid_count
FROM dwh.fact_performance;



-- PERFORMANCE ANALYSIS PREVIEW

-- Top performers by event (optimized with window functions)
WITH ranked_performances AS (
    SELECT 
        e.event_name,
        a.athlete_name,
        a.nationality_code,
        f.result_value,
        f.performance_score,
        f.performance_advantage,
        f.environmental_bonus,
        v.venue_name,
        v.country_name as venue_country,
        v.altitude,
        ROW_NUMBER() OVER (PARTITION BY e.event_name ORDER BY f.performance_score DESC) as performance_rank
    FROM dwh.fact_performance f
    JOIN dwh.dim_athlete a ON f.athlete_key = a.athlete_key
    JOIN dwh.dim_event e ON f.event_key = e.event_key
    JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
    WHERE f.performance_score IS NOT NULL
)
SELECT 
    event_name,
    athlete_name,
    nationality_code,
    ROUND(result_value, 3) as result_value,
    ROUND(performance_score, 1) as performance_score,
    ROUND(performance_advantage, 2) as performance_advantage,
    ROUND(environmental_bonus, 2) as environmental_bonus,
    venue_name,
    venue_country,
    altitude,
    performance_rank
FROM ranked_performances
WHERE performance_rank <= 3  -- Top 3 per event
ORDER BY event_name, performance_rank;



-- FINAL VALIDATION SUMMARY

-- Data completeness report, including calculated measures
SELECT 
    'DATA COMPLETENESS SUMMARY' as report_type,
    metric_name,
    metric_value,
    benchmark,
    status
FROM (
    SELECT 
        'Total Performances' as metric_name,
        COUNT(*)::VARCHAR as metric_value,
        '> 100,000' as benchmark,
        CASE WHEN COUNT(*) > 100000 THEN 'EXCELLENT' ELSE 'GOOD' END as status
    FROM dwh.fact_performance
    
    UNION ALL
    
    SELECT 
        'Unique Athletes',
        COUNT(DISTINCT athlete_key)::VARCHAR,
        '> 10,000',
        CASE WHEN COUNT(DISTINCT athlete_key) > 10000 THEN 'EXCELLENT' ELSE 'GOOD' END
    FROM dwh.fact_performance
    
    UNION ALL
    
    SELECT 
        'Unique Events',
        COUNT(DISTINCT event_key)::VARCHAR,
        '> 20',
        CASE WHEN COUNT(DISTINCT event_key) > 20 THEN 'EXCELLENT' ELSE 'ADEQUATE' END
    FROM dwh.fact_performance
    
    UNION ALL
    
    SELECT 
        'Unique Venues',
        COUNT(DISTINCT venue_key)::VARCHAR,
        '> 1,000',
        CASE WHEN COUNT(DISTINCT venue_key) > 1000 THEN 'EXCELLENT' ELSE 'GOOD' END
    FROM dwh.fact_performance
    
    UNION ALL
    
    SELECT 
        'Performance Scores Complete',
        ROUND(COUNT(performance_score) * 100.0 / COUNT(*), 1)::VARCHAR || '%',
        '> 95%',
        CASE WHEN COUNT(performance_score) * 100.0 / COUNT(*) > 95 THEN 'EXCELLENT' ELSE 'NEEDS IMPROVEMENT' END
    FROM dwh.fact_performance
    
    UNION ALL
    
    SELECT 
        'Environmental Measures Complete',
        ROUND(COUNT(CASE WHEN temperature_impact_factor IS NOT NULL AND environmental_bonus IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)::VARCHAR || '%',
        '> 90%',
        CASE WHEN COUNT(CASE WHEN temperature_impact_factor IS NOT NULL AND environmental_bonus IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) > 90 THEN 'EXCELLENT' ELSE 'NEEDS IMPROVEMENT' END
    FROM dwh.fact_performance
    
    -- UNION ALL
    
    -- SELECT 
    --     'Championship Performances',
    --     COUNT(CASE WHEN is_championship_performance = true THEN 1 END)::VARCHAR,
    --     'Variable',
    --     'INFO ONLY'
    -- FROM dwh.fact_performance
) completeness_metrics
ORDER BY 
    CASE status 
        WHEN 'EXCELLENT' THEN 1 
        WHEN 'GOOD' THEN 2 
        WHEN 'ADEQUATE' THEN 3 
        ELSE 4 
    END,
    metric_name;