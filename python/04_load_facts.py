import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import CONNECTION_STRING
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_db_connection():
    return create_engine(CONNECTION_STRING)



# World Athletics coefficients for scientific performance scoring
WORLD_ATHLETICS_COEFFICIENTS = {
    # Track events (time-based)
    '100m': {'A': 25.4347, 'B': 18.0, 'C': 1.81},
    '200m': {'A': 5.8425, 'B': 38.0, 'C': 1.81},
    '400m': {'A': 1.53775, 'B': 82.0, 'C': 1.81},
    '800m': {'A': 0.11193, 'B': 254.0, 'C': 1.81},
    '1500m': {'A': 0.04491, 'B': 480.0, 'C': 1.81},
    '5000m': {'A': 0.00616, 'B': 2100.0, 'C': 1.81},
    '10000m': {'A': 0.00316, 'B': 4200.0, 'C': 1.81},
    'Marathon': {'A': 0.00024, 'B': 27000.0, 'C': 1.81},
    'Half Marathon': {'A': 0.00058, 'B': 13500.0, 'C': 1.81},
    '110m Hurdles': {'A': 5.74352, 'B': 28.5, 'C': 1.92},
    '100m Hurdles': {'A': 9.23076, 'B': 26.7, 'C': 1.835},
    '400m Hurdles': {'A': 1.4611, 'B': 95.5, 'C': 1.88},
    '3000m Steeplechase': {'A': 0.02883, 'B': 1254.0, 'C': 1.88},
    
    # Field events (distance/height-based)
    'High Jump': {'A': 32.29, 'B': 0.75, 'C': 1.4},
    'Long Jump': {'A': 0.14354, 'B': 1.4, 'C': 1.4},
    'Triple Jump': {'A': 0.03768, 'B': 2.5, 'C': 1.4},
    'Pole Vault': {'A': 39.39, 'B': 1.0, 'C': 1.35},
    'Shot Put': {'A': 51.39, 'B': 1.5, 'C': 1.05},
    'Discus Throw': {'A': 12.91, 'B': 4.0, 'C': 1.1},
    'Hammer Throw': {'A': 13.0449, 'B': 7.0, 'C': 1.05},
    'Javelin Throw': {'A': 15.3, 'B': 7.0, 'C': 1.15},
}


# Event categorization for environmental calculations
EVENT_CATEGORIES = {
    'Sprint': ['100m', '200m', '400m', '100m Hurdles', '110m Hurdles', '400m Hurdles'],
    'Middle Distance': ['800m', '1500m', '3000m', '3000m Steeplechase'],
    'Distance': ['5000m', '10000m', 'Marathon', 'Half Marathon'],
    'Jumps': ['High Jump', 'Long Jump', 'Triple Jump', 'Pole Vault'],
    'Throws': ['Shot Put', 'Discus Throw', 'Hammer Throw', 'Javelin Throw']
}


def get_event_duration_category(event_name):
    """Get event category for environmental adjustments"""
    if pd.isna(event_name):
        return 'Field'
    
    event_str = str(event_name).strip()
    for category, events in EVENT_CATEGORIES.items():
        if any(event.lower() in event_str.lower() for event in events):
            return category
    return 'Field'  # Default


def calculate_temperature_impact_factor(temperature, event_category):
    """Calculate temperature impact factor using scientific research"""
    try:
        if pd.isna(temperature):
            return 1.0  # Neutral impact
        
        # Optimal temperature range: 7-15°C (research-based)
        optimal_temp = 11.0  # Middle of optimal range
        temp_deviation = abs(temperature - optimal_temp)
        
        # Get event duration category for more precise calculation
        duration_category = get_event_duration_category(event_category) if isinstance(event_category, str) else event_category
        
        # Impact rates based on event duration (scientific research)
        if duration_category in ['Sprint', 'Jumps', 'Throws']:
            # Short events: minimal temperature impact
            impact_rate = 0.001  # 0.1% per degree deviation
        elif duration_category == 'Middle Distance':
            # Medium events: moderate impact
            impact_rate = 0.002  # 0.2% per degree deviation
        elif duration_category == 'Distance':
            # Long events: significant impact  
            impact_rate = 0.004  # 0.4% per degree deviation
        else:
            # Default for track events
            impact_rate = 0.002
        
        # Calculate impact factor (minimum 0.5 to avoid extreme values)
        impact_factor = 1.0 - (temp_deviation * impact_rate)
        return max(0.5, min(1.5, impact_factor))
        
    except Exception as e:
        logger.warning(f"Error calculating temperature impact: {e}")
        return 1.0

def calculate_performance_advantage(performance_df):
    """Calculate performance advantage vs venue average with robust statistics"""
    logger.info("Calculating performance advantage vs venue averages...")
    
    # Calculate venue baselines with improved statistics
    venue_baselines = {}
    
    for venue_key in performance_df['venue_key'].unique():
        if pd.isna(venue_key):
            continue
            
        venue_perfs = performance_df[performance_df['venue_key'] == venue_key]
        
        for event_key in venue_perfs['event_key'].unique():
            if pd.isna(event_key):
                continue
                
            event_perfs = venue_perfs[venue_perfs['event_key'] == event_key]
            
            # Require minimum sample size for reliable baseline
            if len(event_perfs) >= 10:  # Increased from 3 for better statistics
                
                # Remove outliers using IQR method
                Q1 = event_perfs['performance_score'].quantile(0.25)
                Q3 = event_perfs['performance_score'].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Filter outliers
                filtered_perfs = event_perfs[
                    (event_perfs['performance_score'] >= lower_bound) & 
                    (event_perfs['performance_score'] <= upper_bound)
                ]
                
                # Require sufficient data after outlier removal
                if len(filtered_perfs) >= 8:
                    avg_score = filtered_perfs['performance_score'].mean()
                    std_score = filtered_perfs['performance_score'].std()

                    # Only store baseline if average is reasonable (prevents division by tiny numbers)
                    if avg_score > 10.0:  # Minimum threshold for reliable percentage calculations
                        venue_baselines[(venue_key, event_key)] = {
                            'avg': avg_score,
                            'std': std_score,
                            'count': len(filtered_perfs)
                        }
    
    # Calculate advantage for each performance
    advantages = []
    for idx, row in performance_df.iterrows():
        venue_key = row['venue_key']
        event_key = row['event_key']
        perf_score = row['performance_score']
        
        if (venue_key, event_key) in venue_baselines:
            baseline = venue_baselines[(venue_key, event_key)]
            venue_avg = baseline['avg']
            
            # Calculate percentage advantage
            if venue_avg > 10.0:   # Ensure reasonable baseline for percentage calculation
                advantage = ((perf_score - venue_avg) / venue_avg) * 100

                # Apply strict bounds to fit DECIMAL(8,3) - max ±99999.999
                advantage = max(-9999.0, min(9999.0, advantage))
            else:
                advantage = 0.0
        else:
            advantage = 0.0  # No reliable comparison data available
            
        advantages.append(advantage)
    
    logger.info(f"Calculated venue baselines for {len(venue_baselines)} venue-event combinations")
    return advantages


def calculate_environmental_bonus(altitude, temperature, event_category):
    """Calculate environmental bonus using scientific altitude and temperature effects"""
    try:
        if pd.isna(altitude) and pd.isna(temperature):
            return 0.0
        
        altitude = altitude if not pd.isna(altitude) else 0
        temperature = temperature if not pd.isna(temperature) else 11.0  # Optimal temp
        
        bonus = 0.0
        duration_category = get_event_duration_category(event_category)
        
        # ALTITUDE EFFECTS (scientific research-based)
        if altitude > 300:  # Apply adjustments only above 300m
            altitude_km = (altitude - 300) / 1000.0
            
            if duration_category == 'Sprint':
                # Sprints benefit from reduced air density (less resistance)
                altitude_bonus = altitude_km * 0.95  # 0.95% improvement per 1000m
            elif duration_category in ['Jumps', 'Throws']:
                # Field events benefit more from reduced air density
                altitude_bonus = altitude_km * 1.2   # 1.2% improvement per 1000m
            elif duration_category in ['Middle Distance', 'Distance']:
                # Endurance events suffer from reduced oxygen
                altitude_bonus = altitude_km * -6.3  # 6.3% decline per 1000m
            else:
                altitude_bonus = 0.0
        else:
            altitude_bonus = 0.0
        
        # TEMPERATURE EFFECTS
        optimal_temp = 11.0  # Research-based optimal temperature
        temp_deviation = abs(temperature - optimal_temp)
        
        if duration_category == 'Distance':
            # Distance events most affected by temperature
            temp_bonus = -temp_deviation * 0.4  # 0.4% per degree deviation
        elif duration_category == 'Middle Distance':
            temp_bonus = -temp_deviation * 0.2  # 0.2% per degree deviation
        else:
            # Sprints and field events less affected
            temp_bonus = -temp_deviation * 0.1  # 0.1% per degree deviation
        
        # Combine effects (scale to reasonable point values)
        total_bonus = (altitude_bonus + temp_bonus) * 2.0  # Scale factor
        
        return max(-20.0, min(20.0, total_bonus))  # Reasonable bounds
        
        # Apply bounds for DECIMAL(8,3) - max ±99999.999
        #return max(-999.0, min(999.0, total_bonus))
        
    except Exception as e:
        logger.warning(f"Error calculating environmental bonus: {e}")
        return 0.0


def calculate_performance_score(result, event_name, unit):
    """Calculate performance score using World Athletics standards"""
    try:
        if pd.isna(result) or pd.isna(event_name):
            return 500.0  # Default score
        
        event_str = str(event_name).strip()
        
        # Try to find exact match in World Athletics coefficients
        coeffs = None
        for wa_event, wa_coeffs in WORLD_ATHLETICS_COEFFICIENTS.items():
            if wa_event.lower() in event_str.lower():
                coeffs = wa_coeffs
                break
        
        if coeffs:
            # Use World Athletics formula: Points = A × |B - T|^C (for time) or A × |T - B|^C (for distance)
            A, B, C = coeffs['A'], coeffs['B'], coeffs['C']
            
            try:
                if unit == 'seconds':
                    # For time events: better time = higher score
                    if result <= 0:
                        return 0.0
                    score = A * pow(abs(B - result), C)
                else:
                    # For distance/height events: better distance = higher score
                    if result <= B:  # Performance must exceed baseline
                        return 0.0
                    score = A * pow(abs(result - B), C)
                
                # Apply reasonable bounds
                return max(0.0, min(1400.0, score))
                
            except (ValueError, OverflowError) as e:
                logger.warning(f"Mathematical error in World Athletics formula for {event_name}: {e}")
                # Fall through to legacy calculation
        
        # Legacy calculation for events not in World Athletics table
        if unit == 'seconds':
            if '100m' in event_str.lower():
                return max(0, 1000 - (result - 9.5) * 200)
            elif '200m' in event_str.lower():
                return max(0, 1000 - (result - 19.0) * 100)
            elif 'mile' in event_str.lower():
                return max(0, 1000 - (result - 220) * 5)
            elif 'marathon' in event_str.lower():
                return max(0, 1000 - (result - 7260) * 0.5)
            else:
                return max(0, 1000 - result * 2)
        else:  # meters
            if 'shot put' in event_str.lower():
                return min(1000, result * 50)
            elif 'javelin' in event_str.lower():
                return min(1000, result * 12.5)
            elif 'high jump' in event_str.lower():
                return min(1000, result * 435)
            elif 'long jump' in event_str.lower():
                return min(1000, result * 118)
            else:
                return min(1000, result * 25)
                
    except Exception as e:
        logger.warning(f"Error calculating performance score for {event_name}: {e}")
        return 500.0


def calculate_altitude_adjustment(result, altitude, event_category):
    """Apply scientifically-based altitude adjustment to performance"""
    try:
        if pd.isna(altitude) or altitude <= 300:  # No adjustment below 300m
            return result
        
        duration_category = get_event_duration_category(event_category)
        altitude_km = (altitude - 300) / 1000.0  # Altitude above 300m baseline
        
        # Scientific altitude adjustment factors
        if duration_category == 'Sprint':
            # Sprints: benefit from reduced air density (less air resistance)
            factor = 1.0 + (altitude_km * 0.0095)  # 0.95% improvement per 1000m
        elif duration_category in ['Jumps', 'Throws']:
            # Field events: benefit more from reduced air density
            factor = 1.0 + (altitude_km * 0.012)   # 1.2% improvement per 1000m
        elif duration_category in ['Middle Distance', 'Distance']:
            # Endurance: hindered by reduced oxygen availability
            factor = 1.0 - (altitude_km * 0.063)   # 6.3% decline per 1000m
        else:
            factor = 1.0  # No adjustment for unknown categories
        
        # Apply adjustment based on measurement unit
        if str(event_category).lower() == 'track' or 'time' in str(event_category).lower():
            # For time-based events: adjustment affects time directly
            adjusted_result = result / factor  # Better factor = lower time
        else:
            # For distance-based events: adjustment affects distance directly  
            adjusted_result = result / factor  # Better factor = higher distance (but we divide due to calculation)
        
        # Apply bounds for DECIMAL(10,3) - ensure reasonable values
        return max(0.0, min(999999.0, adjusted_result))
        
    except Exception as e:
        logger.warning(f"Error calculating altitude adjustment: {e}")
        return result
    
    


def load_fact_table(engine):
    """
    FACT LOADING - 5 Essential Dimensions Only
    Focuses on environmental impact analysis without competition complexity
    
    Dimensions:
    1. WHO - Athlete (athlete_key)
    2. WHAT - Event (event_key) 
    3. WHERE - Venue (venue_key)
    4. WHEN - Date (date_key)
    5. CONDITIONS - Weather (weather_key)
    
    Grain: One performance per athlete/event/venue/date/weather combination
    """
    logger.info("FACT LOADING - 5 Essential Dimensions")


    # Step 1: Load performances (no competition_id needed)
    with engine.connect() as conn:
        perf_query = """
        SELECT 
            athlete_key, event_key, venue_key, weather_key,
            competition_date, result_value, wind_reading, position_finish,
            data_source, data_quality_score, created_date
        FROM reconciled.performances
        WHERE result_value IS NOT NULL
        """
        perf = pd.read_sql(text(perf_query), conn)

    logger.info(f"Loaded {len(perf)} performance records from reconciled layer")


    # Step 2: Load the 5 essential dimensions
    with engine.connect() as conn:
        # WHO - Athlete
        athlete_dim = pd.read_sql(text("""
            SELECT athlete_key, athlete_name, nationality_code, gender, specialization
            FROM dwh.dim_athlete
        """), conn)
        
        # WHAT - Event  
        event_dim = pd.read_sql(text("""
            SELECT event_key, event_name, event_category, measurement_unit, distance_meters
            FROM dwh.dim_event  
        """), conn)
        
        # WHERE - Venue
        venue_dim = pd.read_sql(text("""
            SELECT venue_key, venue_name, city_name, country_code, altitude, climate_zone
            FROM dwh.dim_venue
        """), conn)
        
        # CONDITIONS - Weather
        weather_dim = pd.read_sql(text("""
            SELECT weather_key, venue_name, month_name, temperature
            FROM dwh.dim_weather
        """), conn)

    logger.info("Loaded 5 essential dimensions:")
    logger.info(f"Athletes: {len(athlete_dim)} records")
    logger.info(f"Events: {len(event_dim)} records")
    logger.info(f"Venues: {len(venue_dim)} records")
    logger.info(f"Weather: {len(weather_dim)} records")


    # Step 3: Handle date dimension (WHEN)
    perf['competition_date_parsed'] = pd.to_datetime(perf['competition_date'], errors='coerce')
    
    with engine.connect() as conn:
        date_dim = pd.read_sql(text("""
            SELECT date_key, full_date, year, season, decade
            FROM dwh.dim_date
        """), conn)
    
    date_dim['full_date'] = pd.to_datetime(date_dim['full_date'])
    logger.info(f"Dates: {len(date_dim)} records")


    # Step 4: Join the 5 dimensions (simplified joins)
    logger.info("Joining 5 essential dimensions...")
    
    # WHO - Athlete  
    perf = perf.merge(
        athlete_dim[['athlete_key', 'athlete_name', 'nationality_code', 'gender', 'specialization']], 
        on='athlete_key', how='left'
    )
    
    # WHAT - Event
    perf = perf.merge(
        event_dim[['event_key', 'event_name', 'event_category', 'measurement_unit']], 
        on='event_key', how='left'
    )
    
    # WHERE - Venue
    perf = perf.merge(
        venue_dim[['venue_key', 'venue_name', 'altitude', 'climate_zone']], 
        on='venue_key', how='left'
    )
    
    # CONDITIONS - Weather
    perf = perf.merge(
        weather_dim[['weather_key', 'temperature']], 
        on='weather_key', how='left'
    )
    
    # WHEN - Date
    perf = perf.merge(
        date_dim[['date_key', 'full_date']], 
        left_on='competition_date_parsed', right_on='full_date', how='left'
    )

    logger.info(f"After joining 5 dimensions: {len(perf)} records")

    # Step 5: Check mapping success rates
    key_success = {
        'athlete_key': (~perf['athlete_key'].isna()).sum(),
        'event_key': (~perf['event_key'].isna()).sum(),
        'venue_key': (~perf['venue_key'].isna()).sum(),
        'date_key': (~perf['date_key'].isna()).sum(),
        'weather_key': (~perf['weather_key'].isna()).sum()
    }
    
    logger.info("5-Dimension join success rates:")
    for key, count in key_success.items():
        success_rate = (count / len(perf)) * 100
        logger.info(f"  {key}: {count}/{len(perf)} ({success_rate:.1f}%)")


    # Step 6: Filter out records missing critical dimensions
    initial_count = len(perf)
    perf = perf.dropna(subset=['athlete_key', 'event_key'])
    logger.info(f"Filtered out {initial_count - len(perf)} records missing critical dimensions")


    # Step 7: Calculate ALL measures
    logger.info("Calculating ALL performance measures...")
    
    # Core DFM measures
    logger.info("Calculating performance_score")
    perf['performance_score'] = perf.apply(lambda row:
        calculate_performance_score(row['result_value'], row['event_name'], row['measurement_unit']), axis=1)

    logger.info("Calculating altitude_adjusted_result")
    perf['altitude_adjusted_result'] = perf.apply(lambda row:
        calculate_altitude_adjustment(row['result_value'], row['altitude'], row['event_category']), axis=1)

    # Environmental impact measures
    logger.info("Calculating temperature_impact_factor")
    perf['temperature_impact_factor'] = perf.apply(lambda row:
        calculate_temperature_impact_factor(row['temperature'], row['event_category']), axis=1)
    
    logger.info("Calculating performance_advantage")
    perf['performance_advantage'] = calculate_performance_advantage(perf)
    
    logger.info("Calculating environmental_bonus")
    perf['environmental_bonus'] = perf.apply(lambda row:
        calculate_environmental_bonus(row['altitude'], row['temperature'], row['event_category']), axis=1)


    # Step 8: Add performance context flags (simplified without competition logic)
    logger.info("Adding performance context flags...")
    
    # Simplified championship detection
    #perf['is_championship_performance'] = False  # Simplified - no competition dimension
    
    #perf['is_personal_best'] = False    # TODO: Could calculate based on athlete history
    #perf['is_season_best'] = False      # TODO: Could calculate based on date/athlete
    #perf['is_world_record'] = False     # TODO: Could compare with world record data
    #perf['is_national_record'] = False  # TODO: Could compare with national record data
    perf['has_wind_data'] = pd.notna(perf['wind_reading'])
    perf['load_batch_id'] = 1


    # Step 9: Handle missing dimension keys with defaults
    perf['date_key'] = perf['date_key'].fillna(1)
    perf['venue_key'] = perf['venue_key'].fillna(1)
    perf['weather_key'] = perf['weather_key'].fillna(1)

    # Rename to match schema
    perf['rank_position'] = perf['position_finish']


    # Step 10: Select final columns for fact table
    fact_cols = [
        # 5 ESSENTIAL FOREIGN KEYS
        'athlete_key',      # WHO
        'event_key',        # WHAT
        'venue_key',        # WHERE
        'date_key',         # WHEN
        'weather_key',      # CONDITIONS
        # NOTE: No competition_key - simplified!
        
        # Primary Results
        'result_value', 'rank_position', 'wind_reading',
        
        # Standardized Measures
        'performance_score', 'altitude_adjusted_result',
        
        # Performance Context
        #'is_personal_best', 'is_season_best', 'is_championship_performance',
        #'is_world_record', 'is_national_record',
        
        # Environmental Impact Measures
        'temperature_impact_factor', 'performance_advantage', 'environmental_bonus',
        
        # Additional Quality Measures
        'has_wind_data',
        
        # Data Quality
        'data_quality_score', 'data_source', 'load_batch_id'
    ]

    final_df = perf[fact_cols].copy()


    # Step 11: Simplified success summary
    logger.info("SIMPLIFIED FACT TABLE SUMMARY:")
    logger.info(f"Total performances: {len(final_df):,}")
    logger.info(f"Unique athletes: {final_df['athlete_key'].nunique():,}")
    logger.info(f"Unique events: {final_df['event_key'].nunique():,}")
    logger.info(f"Unique venues: {final_df['venue_key'].nunique():,}")
    logger.info(f"Unique dates: {final_df['date_key'].nunique():,}")
    logger.info(f"Unique weather conditions: {final_df['weather_key'].nunique():,}")
    
    logger.info("ALL CALCULATION FUNCTIONS USED:")
    logger.info(f"performance_score: avg = {final_df['performance_score'].mean():.1f}")
    logger.info(f"altitude_adjusted_result: avg = {final_df['altitude_adjusted_result'].mean():.3f}")
    logger.info(f"temperature_impact_factor: avg = {final_df['temperature_impact_factor'].mean():.3f}")
    logger.info(f"performance_advantage: avg = {final_df['performance_advantage'].mean():.1f}")
    logger.info(f"environmental_bonus: avg = {final_df['environmental_bonus'].mean():.2f}")


    # Step 12: Load to database
    logger.info(f"Loading {len(final_df)} records to dwh.fact_performance...")
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dwh.fact_performance RESTART IDENTITY"))
        conn.commit()
        
        final_df.to_sql('fact_performance', conn, schema='dwh', if_exists='append', index=False)
        conn.commit()
        


def main():
    try:
        logger.info("Starting fact table loading...")
        engine = create_db_connection()
        load_fact_table(engine)

        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_performance")).scalar()
            logger.info(f"Total fact records: {count}")

    except Exception as e:
        logger.error(f"Fact loading failed: {e}")
        raise

if __name__ == "__main__":
    main()
