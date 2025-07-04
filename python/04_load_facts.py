import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import CONNECTION_STRING
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_db_connection():
    return create_engine(CONNECTION_STRING)



def calculate_temperature_impact_factor(temperature, event_category):
    """Calculate temperature impact factor as specified in DFM"""
    try:
        if pd.isna(temperature):
            return 1.0  # Neutral impact
            
        # Optimal temperature ranges by event category
        if event_category == 'Track':
            # Track events: optimal around 15-20°C
            optimal_temp = 17.5
            if 15 <= temperature <= 20:
                return 1.0  # No impact
            elif temperature < 15:
                return 1 - ((15 - temperature) * 0.01)  # Cold penalty
            else:
                return 1 - ((temperature - 20) * 0.015)  # Heat penalty
        else:  # Field events
            # Field events: optimal around 18-25°C  
            optimal_temp = 21.5
            if 18 <= temperature <= 25:
                return 1.0  # No impact
            elif temperature < 18:
                return 1 - ((18 - temperature) * 0.008)  # Cold penalty
            else:
                return 1 - ((temperature - 25) * 0.012)  # Heat penalty
    except:
        return 1.0



def calculate_performance_advantage(performance_df):
    """Calculate performance advantage vs venue average (DFM specification)"""
    logger.info("Calculating performance advantage vs venue averages...")
    
    # Calculate venue averages for each event
    venue_averages = {}
    
    for venue_key in performance_df['venue_key'].unique():
        if pd.isna(venue_key):
            continue
            
        venue_perfs = performance_df[performance_df['venue_key'] == venue_key]
        
        for event_key in venue_perfs['event_key'].unique():
            if pd.isna(event_key):
                continue
                
            event_perfs = venue_perfs[venue_perfs['event_key'] == event_key]
            if len(event_perfs) >= 3:  # Need minimum performances for reliable average
                avg_score = event_perfs['performance_score'].mean()
                venue_averages[(venue_key, event_key)] = avg_score
    
    # Calculate advantage for each performance
    advantages = []
    for idx, row in performance_df.iterrows():
        venue_key = row['venue_key']
        event_key = row['event_key']
        perf_score = row['performance_score']
        
        if (venue_key, event_key) in venue_averages:
            venue_avg = venue_averages[(venue_key, event_key)]
            advantage = perf_score - venue_avg
        else:
            advantage = 0.0  # No comparison data available
            
        advantages.append(advantage)
    
    return advantages



def calculate_environmental_bonus(altitude, temperature, event_category):
    """Calculate environmental bonus from combined altitude/temperature effects"""
    try:
        if pd.isna(altitude) or pd.isna(temperature):
            return 0.0
            
        bonus = 0.0
        
        # Altitude effects (beneficial for some events at moderate altitude)
        if event_category == 'Track':
            # Sprints benefit slightly from altitude (less air resistance)
            if 500 <= altitude <= 1500:
                bonus += (altitude / 1000) * 0.5  # Small bonus
            elif altitude > 1500:
                bonus -= (altitude - 1500) / 1000 * 0.3  # Penalty for extreme altitude
        else:  # Field events
            # Throws/jumps benefit more from altitude
            if 500 <= altitude <= 2000:
                bonus += (altitude / 1000) * 1.0  # Larger bonus
            elif altitude > 2000:
                bonus -= (altitude - 2000) / 1000 * 0.5  # Penalty for extreme altitude
        
        # Temperature effects (moderate temperatures are best)
        temp_bonus = 0.0
        if 15 <= temperature <= 25:
            temp_bonus = 1.0  # Optimal range
        elif temperature < 15:
            temp_bonus = 0.5 - ((15 - temperature) * 0.05)  # Cold penalty
        else:
            temp_bonus = 0.5 - ((temperature - 25) * 0.03)  # Heat penalty
        
        return bonus * max(0, temp_bonus)
    except:
        return 0.0
    


def calculate_performance_score(result, event_name, unit):
    """Calculate performance score based on event type and result"""
    try:
        if pd.isna(result):
            return 500
            
        if unit == 'seconds':
            if '100m' in str(event_name).lower():
                # 9.5s = 1000 points, each 0.01s slower = -2 points
                return max(0, 1000 - (result - 9.5) * 200)
            elif '200m' in str(event_name).lower():
                # 19.0s = 1000 points, each 0.01s slower = -1 point
                return max(0, 1000 - (result - 19.0) * 100)
            elif 'mile' in str(event_name).lower():
                # 3:40 (220s) = 1000 points
                return max(0, 1000 - (result - 220) * 5)
            elif 'marathon' in str(event_name).lower():
                # 2:01:00 (7260s) = 1000 points
                return max(0, 1000 - (result - 7260) * 0.5)
            else:
                # Generic time-based scoring
                return max(0, 1000 - result * 2)
        else:  # meters (field events)
            # Higher distance/height = better score
            if 'shot put' in str(event_name).lower():
                # 20m = 1000 points
                return min(1000, result * 50)
            elif 'javelin' in str(event_name).lower():
                # 80m = 1000 points  
                return min(1000, result * 12.5)
            elif 'high jump' in str(event_name).lower():
                # 2.3m = 1000 points
                return min(1000, result * 435)
            elif 'long jump' in str(event_name).lower():
                # 8.5m = 1000 points
                return min(1000, result * 118)
            else:
                # Generic distance-based scoring
                return min(1000, result * 25)
    except Exception as e:
        logger.warning(f"Error calculating performance score: {e}")
        return 500



def calculate_altitude_adjustment(result, altitude, event_category):
    """Apply altitude adjustment to performance"""
    try:
        if pd.isna(altitude) or altitude == 0:
            return result
            
        # Altitude affects performance differently for track vs field
        if event_category == 'Track':
            # Track events: thinner air = faster times (negative adjustment for sprints)
            factor = 1 - (altitude / 10000) * 0.005  # 0.5% per 1000m
        else:
            # Field events: thinner air = longer throws/jumps (positive adjustment)
            factor = 1 + (altitude / 10000) * 0.01   # 1% per 1000m
            
        return result * factor
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
    perf['is_championship_performance'] = False  # Simplified - no competition dimension
    
    perf['is_personal_best'] = False    # TODO: Could calculate based on athlete history
    perf['is_season_best'] = False      # TODO: Could calculate based on date/athlete
    perf['is_world_record'] = False     # TODO: Could compare with world record data
    perf['is_national_record'] = False  # TODO: Could compare with national record data
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
        'is_personal_best', 'is_season_best', 'is_championship_performance',
        'is_world_record', 'is_national_record',
        
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
