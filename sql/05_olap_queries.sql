-- OLAP Analysis Queries for 3-Layer Athletics Environmental Impact DW
-- Demonstrates advanced queries across all three layers

-- ===========================
-- LAYER VERIFICATION QUERIES
-- ===========================

-- Verify 3-layer architecture data flow
SELECT 
    'Layer 1 (Staging)' as layer,
    'raw_world_athletics' as table_name,
    COUNT(*) as records
FROM staging.raw_world_athletics
UNION ALL
SELECT 
    'Layer 2 (Reconciled)',
    'performances',
    COUNT(*)
FROM reconciled.performances
UNION ALL
SELECT 
    'Layer 3 (Star Schema)',
    'fact_performance',
    COUNT(*)
FROM dwh.fact_performance
ORDER BY layer;

-- Data quality progression across layers with calculated measures
SELECT 
    'Staging Completeness' as metric,
    ROUND(AVG(CASE WHEN athlete_name IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as percentage,
    COUNT(DISTINCT athlete_name) as unique_count
FROM staging.clean_world_athletics
UNION ALL
SELECT 
    'Reconciled Quality Score',
    ROUND(AVG(data_quality_score) * 10, 1),
    COUNT(DISTINCT athlete_key)
FROM reconciled.performances
UNION ALL
SELECT 
    'Star Schema Quality Score',
    ROUND(AVG(data_quality_score) * 10, 1),
    COUNT(DISTINCT athlete_key)
FROM dwh.fact_performance
UNION ALL
SELECT 
    'Performance Score Completeness',
    ROUND(COUNT(CASE WHEN performance_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1),
    COUNT(DISTINCT athlete_key)
FROM dwh.fact_performance
UNION ALL
SELECT 
    'Environmental Measures Completeness',
    ROUND(COUNT(CASE WHEN temperature_impact_factor IS NOT NULL AND environmental_bonus IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1),
    COUNT(DISTINCT venue_key)
FROM dwh.fact_performance;



-- ===========================
-- ADVANCED OLAP QUERY 1: DRILL-DOWN ANALYSIS
-- Environmental Impact with Geographic Hierarchy × Altitude Effects
-- ===========================

-- How does environmental impact vary across geographic hierarchy?
-- Country → City → Venue × Altitude Category
-- Ordered by performance metrics to show most interesting results first
SELECT 
    COALESCE(v.country_name, 'ALL COUNTRIES') as country,
    COALESCE(v.city_name, 'ALL CITIES') as city,
    COALESCE(v.venue_name, 'ALL VENUES') as venue,
    COALESCE(v.altitude_category, 'ALL ALTITUDES') as altitude_category,
    COUNT(*) as total_performances,
    ROUND(AVG(f.performance_score), 2) as avg_performance_score,
    ROUND(AVG(f.altitude_adjusted_result), 3) as avg_altitude_adjusted,
    ROUND(AVG(f.result_value), 3) as avg_raw_result,
    ROUND(AVG(v.altitude), 0) as avg_altitude_meters,
    ROUND(AVG(f.performance_advantage), 2) as avg_performance_advantage,
    ROUND(AVG(f.environmental_bonus), 2) as avg_environmental_bonus,
    ROUND(AVG(f.temperature_impact_factor), 3) as avg_temp_factor,
    -- Altitude impact calculation
    ROUND(AVG(f.altitude_adjusted_result - f.result_value), 3) as avg_altitude_adjustment,
    ROUND((AVG(f.altitude_adjusted_result) - AVG(f.result_value)) / AVG(f.result_value) * 100, 2) as altitude_impact_pct,
    -- Performance quality indicators
    COUNT(CASE WHEN f.performance_score > 1000 THEN 1 END) as elite_performances,
    ROUND(100.0 * COUNT(CASE WHEN f.performance_score > 1000 THEN 1 END) / COUNT(*), 1) as elite_percentage,
    COUNT(CASE WHEN f.performance_score > 800 THEN 1 END) as competitive_performances,
    ROUND(100.0 * COUNT(CASE WHEN f.performance_score > 800 THEN 1 END) / COUNT(*), 1) as competitive_percentage,
    -- Analysis level indicator
    CASE 
        WHEN v.venue_name IS NOT NULL THEN 'Venue Level'
        WHEN v.city_name IS NOT NULL THEN 'City Level'
        WHEN v.country_name IS NOT NULL THEN 'Country Level'
        ELSE 'Global Level'
    END as analysis_level
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
WHERE f.performance_score IS NOT NULL
  AND v.country_name IS NOT NULL
  AND v.country_name != 'Unknown'
GROUP BY ROLLUP(v.country_name, v.city_name, v.venue_name), v.altitude_category
HAVING COUNT(*) >= 15  -- Minimum threshold for reliable statistics
-- ORDER BY PERFORMANCE METRICS FIRST to show most interesting results
ORDER BY 
    avg_performance_score DESC,           -- Best performing venues first
    total_performances DESC,              -- Most active venues second
    avg_performance_advantage DESC        -- Best venue advantages third
LIMIT 60;

--- Query to investigate events with capped scores (a problem for the first query)
SELECT 
    e.event_name,
    e.measurement_unit,
    COUNT(*) as capped_count,
    AVG(f.result_value) as avg_result_value,
    MIN(f.result_value) as min_result,
    MAX(f.result_value) as max_result
FROM dwh.fact_performance f
JOIN dwh.dim_event e ON f.event_key = e.event_key  
WHERE f.performance_score = 1400
GROUP BY e.event_name, e.measurement_unit
ORDER BY capped_count DESC;
---


-- ===========================
-- SUPPLEMENTARY: TOP PERFORMING COUNTRIES ANALYSIS
-- Country-Level Environmental Performance Summary
-- ===========================

-- Which countries show the best environmental adaptation?
SELECT 
    v.country_name,
    v.country_code,
    COUNT(*) as total_performances,
    COUNT(DISTINCT v.venue_key) as venues_count,
    COUNT(DISTINCT f.athlete_key) as athletes_count,
    COUNT(DISTINCT f.event_key) as events_count,
    ROUND(AVG(f.performance_score), 2) as avg_performance_score,
    ROUND(AVG(f.performance_advantage), 2) as avg_performance_advantage,
    ROUND(AVG(f.environmental_bonus), 2) as avg_environmental_bonus,
    ROUND(AVG(v.altitude), 0) as avg_country_altitude,
    -- Performance distribution
    COUNT(CASE WHEN f.performance_score > 1000 THEN 1 END) as elite_count,
    COUNT(CASE WHEN f.performance_score BETWEEN 800 AND 1000 THEN 1 END) as competitive_count,
    COUNT(CASE WHEN f.performance_score < 600 THEN 1 END) as recreational_count,
    -- Environmental adaptability score
    ROUND(AVG(f.environmental_bonus) + AVG(f.performance_advantage) * 0.1, 2) as adaptability_score,
    -- Country performance rating
    CASE 
        WHEN AVG(f.performance_score) > 900 THEN 'Elite Nation'
        WHEN AVG(f.performance_score) > 750 THEN 'Strong Nation'
        WHEN AVG(f.performance_score) > 600 THEN 'Competitive Nation'
        ELSE 'Developing Nation'
    END as country_rating
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
WHERE f.performance_score IS NOT NULL
  AND v.country_name IS NOT NULL
  AND v.country_name != 'Unknown'
GROUP BY v.country_name, v.country_code
HAVING COUNT(*) >= 50  -- Countries with significant data
ORDER BY avg_performance_score DESC
LIMIT 25;



-- ===========================
-- ADVANCED OLAP QUERY 2: TIME SERIES WITH ROLL-UP
-- Performance Evolution Across Competition Hierarchy
-- ===========================
-- ADVANCED OLAP QUERY 2: TIME SERIES ENVIRONMENTAL ANALYSIS (UPDATED)
-- Performance Evolution Across Time × Environmental Conditions
-- ===========================

-- How has performance evolved across time and environmental conditions?
-- Decade → Year → Season × Event Groups
SELECT 
    COALESCE(d.decade, 'ALL DECADES') as decade,
    COALESCE(CAST(d.year AS VARCHAR), 'ALL YEARS') as year,
    COALESCE(d.season, 'ALL SEASONS') as season,
    e.event_category,
    COUNT(*) as performances,
    ROUND(AVG(f.performance_score), 2) as avg_score,
    ROUND(MIN(f.performance_score), 2) as min_score,
    ROUND(MAX(f.performance_score), 2) as max_score,
    ROUND(STDDEV(f.performance_score), 2) as score_variance,
    ROUND(AVG(f.temperature_impact_factor), 3) as avg_temp_impact,
    ROUND(AVG(f.environmental_bonus), 2) as avg_env_bonus,
    ROUND(AVG(f.performance_advantage), 2) as avg_advantage,
    -- Moving average calculation
    ROUND(AVG(AVG(f.performance_score)) OVER (
        PARTITION BY e.event_category 
        ORDER BY d.decade, d.year 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) as moving_avg_3period
FROM dwh.fact_performance f
JOIN dwh.dim_date d ON f.date_key = d.date_key
JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE d.year >= 2010
  AND e.event_category IN ('Track', 'Field')
  AND f.performance_score IS NOT NULL
GROUP BY ROLLUP(d.decade, d.year, d.season), e.event_category
HAVING COUNT(*) >= 5
ORDER BY decade NULLS LAST, year NULLS LAST, season NULLS LAST, e.event_category;



-- ===========================
-- ADVANCED OLAP QUERY 3: SLICE AND DICE ANALYSIS
-- Multi-Dimensional Environmental Performance Analysis
-- Da rivedere, produce risultati inaspettati, ma per accertarlo bisogna prima fixare tutti i punteggi delle gare (anche femminili) 
-- ===========================

-- Environmental Conditions Impact: Clean Slice and Dice Analysis
SELECT 
    v.altitude_category,
    w.temperature_category,
    e.event_category,
    e.measurement_unit,
    COUNT(*) as performance_count,
    ROUND(AVG(f.performance_score), 2) as avg_score,
    ROUND(AVG(f.result_value), 3) as avg_result,
    ROUND(AVG(f.altitude_adjusted_result), 3) as avg_altitude_adjusted,
    ROUND(AVG(f.temperature_impact_factor), 3) as avg_temp_factor,
    ROUND(AVG(f.environmental_bonus), 2) as avg_env_bonus,
    ROUND(AVG(f.performance_advantage), 2) as avg_advantage,
    -- Environmental impact calculation (FIXED: handle division by zero)
    CASE 
        WHEN AVG(f.result_value) > 0 THEN 
            ROUND((AVG(f.altitude_adjusted_result) - AVG(f.result_value)) / AVG(f.result_value) * 100, 2)
        ELSE NULL 
    END as altitude_impact_pct,
    -- Consistency measure (FIXED: handle division by zero)
    CASE 
        WHEN AVG(f.performance_score) > 0 THEN 
            ROUND(STDDEV(f.performance_score) / AVG(f.performance_score) * 100, 2)
        ELSE NULL 
    END as coefficient_variation,
    -- Environmental rating
    CASE 
        WHEN v.altitude_category = 'High' AND w.temperature_category = 'Cool' THEN 'Optimal for Field Events'
        WHEN v.altitude_category = 'Moderate' AND w.temperature_category = 'Cool' THEN 'Optimal for Track Events'
        WHEN w.temperature_category = 'Hot' THEN 'Challenging - Heat Stress'
        WHEN w.temperature_category = 'Cold' THEN 'Challenging - Cold Stress'
        WHEN v.altitude_category = 'High' THEN 'Challenging for Endurance'
        ELSE 'Standard Conditions'
    END as environmental_rating,
    -- Performance classification
    CASE 
        WHEN AVG(f.performance_score) > 1000 THEN 'Elite Conditions'
        WHEN AVG(f.performance_score) > 900 THEN 'Excellent Conditions'
        WHEN AVG(f.performance_score) > 800 THEN 'Good Conditions'
        ELSE 'Challenging Conditions'
    END as performance_classification
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
JOIN dwh.dim_weather w ON f.weather_key = w.weather_key
JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE f.performance_score IS NOT NULL
  AND v.altitude_category IS NOT NULL
  AND w.temperature_category IS NOT NULL
  -- Additional filters to avoid problematic data
  AND f.result_value > 0
  AND f.performance_score > 0
-- SLICE AND DICE: Only specific combinations (no CUBE subtotals)
GROUP BY v.altitude_category, w.temperature_category, e.event_category, e.measurement_unit
HAVING COUNT(*) >= 15
ORDER BY avg_score DESC
LIMIT 30;



-- ===========================
-- ADVANCED OLAP QUERY 4: PIVOTING ANALYSIS
-- Venue Performance Matrix (Pivot View)
-- ===========================

-- Pivot: Venues vs Event Groups performance comparison
SELECT 
    venue_name,
    country_name,
    altitude_category,
    ROUND(AVG(CASE WHEN event_group = 'Sprint' THEN performance_score END), 2) as sprint_avg,
    ROUND(AVG(CASE WHEN event_group = 'Distance' THEN performance_score END), 2) as distance_avg,
    ROUND(AVG(CASE WHEN event_group = 'Jumps' THEN performance_score END), 2) as jumps_avg,
    ROUND(AVG(CASE WHEN event_group = 'Throws' THEN performance_score END), 2) as throws_avg,
    COUNT(CASE WHEN event_group = 'Sprint' THEN 1 END) as sprint_count,
    COUNT(CASE WHEN event_group = 'Distance' THEN 1 END) as distance_count,
    COUNT(CASE WHEN event_group = 'Jumps' THEN 1 END) as jumps_count,
    COUNT(CASE WHEN event_group = 'Throws' THEN 1 END) as throws_count,
    -- Overall venue score
    ROUND(AVG(performance_score), 2) as overall_venue_score,
    -- Venue specialization index (which event type performs best)
    CASE 
        WHEN AVG(CASE WHEN event_group = 'Sprint' THEN performance_score END) = 
             GREATEST(
                AVG(CASE WHEN event_group = 'Sprint' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Distance' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Jumps' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Throws' THEN performance_score END)
             ) THEN 'Sprint Specialist'
        WHEN AVG(CASE WHEN event_group = 'Distance' THEN performance_score END) = 
             GREATEST(
                AVG(CASE WHEN event_group = 'Sprint' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Distance' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Jumps' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Throws' THEN performance_score END)
             ) THEN 'Distance Specialist'
        WHEN AVG(CASE WHEN event_group = 'Jumps' THEN performance_score END) = 
             GREATEST(
                AVG(CASE WHEN event_group = 'Sprint' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Distance' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Jumps' THEN performance_score END),
                AVG(CASE WHEN event_group = 'Throws' THEN performance_score END)
             ) THEN 'Jumps Specialist'
        ELSE 'Throws Specialist'
    END as venue_specialization
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE v.venue_name != 'Unknown'
  AND f.performance_score IS NOT NULL
GROUP BY v.venue_name, v.country_name, v.altitude_category
HAVING COUNT(*) >= 12
   AND COUNT(DISTINCT e.event_group) >= 2  -- Venue must host multiple event types
ORDER BY overall_venue_score DESC
LIMIT 25;




-- ===========================
-- ADVANCED OLAP QUERY 5: WINDOW FUNCTIONS & RANKING
-- Athlete Performance Progression Analysis
-- da verificare efficacia
-- ===========================

-- Advanced ranking and progression analysis
WITH athlete_performance AS (
    SELECT 
        a.athlete_name,
        a.nationality_code,  -- Fixed: using actual column name
        e.event_name,
        e.event_category,    -- Fixed: using actual column name  
        d.year,
        f.performance_score,
        f.result_value,
        -- Ranking within event and year
        ROW_NUMBER() OVER (PARTITION BY e.event_name, d.year ORDER BY f.performance_score DESC) as yearly_rank,
        -- Performance percentile within event
        PERCENT_RANK() OVER (PARTITION BY e.event_name ORDER BY f.performance_score DESC) as performance_percentile,
        -- Moving average of athlete's performances
        AVG(f.performance_score) OVER (
            PARTITION BY a.athlete_name, e.event_name 
            ORDER BY d.year 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as performance_moving_avg,
        -- Year-over-year improvement
        LAG(f.performance_score) OVER (
            PARTITION BY a.athlete_name, e.event_name 
            ORDER BY d.year
        ) as previous_year_score,
        -- Best performance to date
        MAX(f.performance_score) OVER (
            PARTITION BY a.athlete_name, e.event_name 
            ORDER BY d.year 
            ROWS UNBOUNDED PRECEDING
        ) as best_score_to_date
    FROM dwh.fact_performance f
    JOIN dwh.dim_athlete a ON f.athlete_key = a.athlete_key
    JOIN dwh.dim_event e ON f.event_key = e.event_key
    JOIN dwh.dim_date d ON f.date_key = d.date_key
    WHERE f.performance_score IS NOT NULL
      AND d.year >= 2015
)
SELECT 
    athlete_name,
    nationality_code,
    event_name,
    year,
    ROUND(performance_score::NUMERIC, 2) as current_score,
    yearly_rank,
    -- FIXED: Cast to NUMERIC before rounding
    ROUND((performance_percentile * 100)::NUMERIC, 1) as percentile_rank,
    ROUND(performance_moving_avg::NUMERIC, 2) as moving_avg,
    -- FIXED: Handle NULL values properly
    CASE 
        WHEN previous_year_score IS NOT NULL THEN 
            ROUND((performance_score - previous_year_score)::NUMERIC, 2)
        ELSE NULL
    END as year_over_year_change,
    ROUND((performance_score - best_score_to_date)::NUMERIC, 2) as improvement_from_best,
    CASE 
        WHEN previous_year_score IS NULL THEN 'Debut Year'
        WHEN performance_score > previous_year_score THEN 'Improving'
        WHEN performance_score < previous_year_score THEN 'Declining'
        ELSE 'Stable'
    END as performance_trend
FROM athlete_performance
WHERE yearly_rank <= 5  -- Top 5 in each event/year
  AND performance_score IS NOT NULL
ORDER BY event_name, year DESC, yearly_rank;



-- ===========================
-- BUSINESS INSIGHTS SUMMARY QUERIES
-- ===========================

-- Executive Summary: Key Performance Indicators        (doesn't work)
SELECT 
    'Total Performances Analyzed' as kpi,
    COUNT(*)::VARCHAR as value,
    'Records' as unit
FROM dwh.fact_performance
UNION ALL
SELECT 
    'Average Performance Score',
    ROUND(AVG(performance_score), 2)::VARCHAR,
    'Points (0-1000)'
FROM dwh.fact_performance
UNION ALL
SELECT 
    'Best Performing Altitude Category',
    v.altitude_category,
    ROUND(AVG(f.performance_score), 2)::VARCHAR || ' avg score'
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
WHERE v.altitude_category != 'Unknown'
GROUP BY v.altitude_category
ORDER BY AVG(f.performance_score) DESC
LIMIT 1
UNION ALL
SELECT 
    'Optimal Temperature Range',
    w.temperature_category,
    ROUND(AVG(f.performance_score), 2)::VARCHAR || ' avg score'
FROM dwh.fact_performance f
JOIN dwh.dim_weather w ON f.weather_key = w.weather_key
WHERE w.temperature_category != 'Unknown'
GROUP BY w.temperature_category
ORDER BY AVG(f.performance_score) DESC
LIMIT 1
UNION ALL
SELECT 
    'Elite vs Amateur Performance Gap',
    ROUND(
        (SELECT AVG(performance_score) FROM dwh.fact_performance f JOIN dwh.dim_competition c ON f.competition_key = c.competition_key WHERE c.competition_level = 'Elite') -
        (SELECT AVG(performance_score) FROM dwh.fact_performance f JOIN dwh.dim_competition c ON f.competition_key = c.competition_key WHERE c.competition_level = 'Amateur'), 2
    )::VARCHAR,
    'Score Points'
UNION ALL
SELECT 
    'Countries Represented',
    COUNT(DISTINCT country_name)::VARCHAR,
    'Nations'
FROM dwh.dim_venue v
JOIN dwh.fact_performance f ON v.venue_key = f.venue_key
WHERE v.country_name != 'Unknown';


-- Actionable Business Recommendations
SELECT 
    'RECOMMENDATION' as insight_type,
    'Event Type' as category,
    'Optimal Conditions' as subcategory,
    CONCAT(
        'For ', e.event_group, ' events, ',
        'altitude category "', v.altitude_category, '" shows ',
        ROUND(AVG(f.performance_score), 1), ' average score - ',
        CASE 
            WHEN AVG(f.performance_score) > 700 THEN 'HIGHLY RECOMMENDED'
            WHEN AVG(f.performance_score) > 600 THEN 'RECOMMENDED'
            ELSE 'AVOID'
        END
    ) as recommendation
FROM dwh.fact_performance f
JOIN dwh.dim_venue v ON f.venue_key = v.venue_key
JOIN dwh.dim_event e ON f.event_key = e.event_key
WHERE v.altitude_category != 'Unknown'
  AND e.event_group IN ('Sprint', 'Distance', 'Jumps', 'Throws')
GROUP BY e.event_group, v.altitude_category
HAVING COUNT(*) >= 10
ORDER BY e.event_group, AVG(f.performance_score) DESC;