import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import CONNECTION_STRING
import logging
import calendar

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_db_connection():
    return create_engine(CONNECTION_STRING)

def reconcile_athletes(engine):
    logger.info("Reconciling athletes...")

    query = """
    SELECT DISTINCT
        athlete_name,
        nationality,
        gender,
        data_source
    FROM staging.clean_world_athletics
    WHERE athlete_name IS NOT NULL
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    df['athlete_name_clean'] = df['athlete_name'].str.strip().str.title()
    df['nationality_standardized'] = df['nationality'].str.strip().str.title()
    df['birth_decade'] = 'Unknown'
    df['specialization'] = 'All-around'
    df['data_quality_score'] = 8
    df['source_system'] = df['data_source']
    df['nationality_code'] = df['nationality'].str.upper().str[:3]

    # FIX: Convert gender to single character
    def normalize_gender(gender_val):
        if pd.isna(gender_val):
            return 'U'  # Unknown
        gender_str = str(gender_val).lower().strip()
        if gender_str in ['female', 'f', 'w']:
            return 'F'
        elif gender_str in ['male', 'm']:
            return 'M'
        else:
            return 'U'  # Unknown
    
    df['gender_normalized'] = df['gender'].apply(normalize_gender)

    final = df[['athlete_name', 'athlete_name_clean', 'nationality_standardized', 'nationality_code',
                'gender_normalized', 'birth_decade', 'specialization',  # Use gender_normalized
                'data_quality_score', 'source_system']]
    
    # Rename to match table schema
    final = final.rename(columns={'gender_normalized': 'gender'})

    with engine.connect() as conn:
        final.to_sql('athletes', conn, schema='reconciled', if_exists='append', index=False)
        conn.commit()
    
    logger.info(f"Inserted {len(final)} athletes.")
    return final



def reconcile_events(engine):
    logger.info("Reconciling events...")

    query = """
    SELECT DISTINCT event_clean as event_name
    FROM staging.clean_world_athletics
    WHERE event_clean IS NOT NULL
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    def categorize(event):
        e = event.lower()
        # CHECK HURDLES FIRST - before checking distances
        if 'hurdles' in e:
            return 'Hurdles'
        elif 'jump' in e:
            return 'Jumps'
        elif 'throw' in e or 'put' in e:
            return 'Throws'
        # Check distances AFTER hurdles
        elif any(x in e for x in ['100m', '200m', '400m', '60m']):
            return 'Sprint'
        elif any(x in e for x in ['800m', '1500m', '5000m', '10000m', 'marathon']):
            return 'Distance'
        else:
            return 'Other'

    def extract_distance(event):
        import re
        match = re.search(r'(\d+)(?=m)', event.lower())
        return int(match.group(1)) if match else None

    df['event_name_standardized'] = df['event_name']
    df['event_group'] = df['event_name'].apply(categorize)
    
    # Fix categorization logic - Hurdles should be Track, not Field
    def get_category(group):
        if group in ['Sprint', 'Distance', 'Hurdles']:
            return 'Track'
        else:
            return 'Field'
    
    df['event_category'] = df['event_group'].apply(get_category)
    df['is_outdoor_event'] = True
    
    # Fix measurement unit logic
    def get_measurement_unit(category):
        if category == 'Track':
            return 'seconds'
        else:
            return 'meters'
    
    df['measurement_unit'] = df['event_category'].apply(get_measurement_unit)
    df['distance_meters'] = df['event_name'].apply(extract_distance)
    
    # FIX: Use shorter gender value to fit VARCHAR(10)
    df['gender'] = 'Mixed'  # 5 characters - fits in VARCHAR(10)
    
    df['world_record'] = None

    final = df[['event_name', 'event_name_standardized', 'event_group', 'event_category',
                'distance_meters', 'measurement_unit', 'gender',
                'is_outdoor_event', 'world_record']]

    with engine.connect() as conn:
        final.to_sql('events', conn, schema='reconciled', if_exists='append', index=False)
        conn.commit()
    
    logger.info(f"Inserted {len(final)} events.")
    return final



def reconcile_venues(engine):
    logger.info("Reconciling venues...")

    # Step 1: Get unique venues (fast)
    venues_query = """
    SELECT DISTINCT venue_name
    FROM staging.clean_world_athletics
    WHERE venue_name IS NOT NULL
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(text(venues_query), conn)

    logger.info(f"Processing {len(df)} unique venues...")

    # Step 2: Extract city/country from venue names (fast Python parsing)
    location_info = df['venue_name'].apply(extract_location_from_venue)
    
    df['city_extracted'] = [info['city'] for info in location_info]
    df['country_extracted'] = [info['country'] for info in location_info]
    df['country_code'] = [info['country_code'] for info in location_info]

    # Clean extracted city names for matching
    df['city_extracted_clean'] = df['city_extracted'].str.strip().str.upper()

    # Step 3: Get cities data for matching (one-time read)
    cities_query = """
    SELECT DISTINCT 
        city_name,
        country_name, 
        latitude,
        longitude
    FROM staging.clean_cities
    WHERE city_name IS NOT NULL
    """
    
    with engine.connect() as conn:
        cities_df = pd.read_sql(text(cities_query), conn)
    
    # Prepare cities data for matching
    cities_df['city_name_clean'] = cities_df['city_name'].str.strip().str.upper()

    logger.info(f"Matching against {len(cities_df)} cities...")

    # Step 4: Join venues with cities (much smaller join - ~1000 venues vs 3.2M cities)
    merged_df = df.merge(
        cities_df, 
        left_on='city_extracted_clean', 
        right_on='city_name_clean', 
        how='left'
    )

    # Step 5: Build final venue data with real geographic attributes
    merged_df['venue_name_clean'] = merged_df['venue_name'].str.strip().str.title()
    
    # Use matched city data or extracted data as fallback
    merged_df['city_name'] = merged_df['city_name'].fillna(merged_df['city_extracted'])
    merged_df['country_name'] = merged_df['country_name'].fillna(merged_df['country_extracted'])
    
    # Geographic attributes from cities database
    merged_df['latitude'] = merged_df['latitude'].fillna(0.0)
    merged_df['longitude'] = merged_df['longitude'].fillna(0.0)

    # Calculate altitude from latitude (your existing function)
    def estimate_altitude(lat):
        if pd.isna(lat) or lat == 0.0: 
            return 100
        return max(0, int(abs(lat) * 50))

    def categorize_altitude_from_value(alt):
        if alt > 1500:
            return 'High'
        elif alt > 500:
            return 'Moderate'
        else:
            return 'Sea Level'

    merged_df['altitude'] = merged_df['latitude'].apply(estimate_altitude)
    merged_df['altitude_category'] = merged_df['altitude'].apply(categorize_altitude_from_value)
    
    # Other attributes
    merged_df['continent'] = 'Unknown'  # Could be enhanced based on country
    merged_df['city_size'] = 'Unknown'
    merged_df['population'] = 0
    merged_df['data_quality_score'] = merged_df.apply(
        lambda row: 8 if not pd.isna(row['latitude']) and row['latitude'] != 0 else 6, axis=1
    )
    merged_df['geographic_source'] = 'Venue_Parsing_Plus_Cities_DB'

    # Select final columns
    final = merged_df[['venue_name', 'venue_name_clean', 'city_name', 'country_name', 'country_code',
                       'latitude', 'longitude', 'altitude', 'altitude_category',
                       'continent', 'city_size', 'population',
                       'data_quality_score', 'geographic_source']]

    with engine.connect() as conn:
        final.to_sql('venues', conn, schema='reconciled', if_exists='append', index=False)
        conn.commit()
    
    logger.info(f"Inserted {len(final)} venues with geographic data.")
    
    # Show match success rate
    matched_count = len(final[final['latitude'] != 0.0])
    logger.info(f"Successfully matched {matched_count}/{len(final)} venues to geographic data")
    
    return final



def reconcile_weather(engine):
    logger.info("Reconciling weather conditions...")

    query = """
    SELECT DISTINCT
        "City" as venue_name,   -- Map city to venue_name as per table schema
        "Month" as month,
        "AvgTemperature" as temperature,
        temperature_category,
        data_source
    FROM staging.clean_temperature
    WHERE "City" IS NOT NULL AND "Month" IS NOT NULL
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    df = df.dropna(subset=['venue_name', 'month'])

    df['month_name'] = df['month'].apply(lambda x: calendar.month_name[int(x)] if not pd.isna(x) else 'Unknown')

    # Add season category
    def get_season(month):
        if pd.isna(month):
            return 'Unknown'
        month = int(month)
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'
    
    df['season_category'] = df['month'].apply(get_season)

    df['has_actual_data'] = True  # All temperature data is actual
    df['weather_source'] = 'data_source'

    final = df[['venue_name', 'month_name', 'temperature',
                'temperature_category', 'season_category',
                'has_actual_data', 'weather_source']]
    

    with engine.connect() as conn:
       final.to_sql('weather_conditions', conn, schema='reconciled', if_exists='append', index=False)
       conn.commit()

    logger.info(f"Inserted {len(final)} weather records.")
    return final



def reconcile_competitions(engine):
    logger.info("Creating simple default competition...")
    
    # Create just ONE default competition for all performances, we don't really need to know in what occasion a result has been scored
    competition_data = [{
        'competition_name': 'Athletics Competition',
        'competition_type': 'General Meeting', 
        'competition_level': 'Professional',
        'prestige_level': 3,
        'is_indoor': False
    }]
    
    df = pd.DataFrame(competition_data)
    
    with engine.connect() as conn:
        df.to_sql('competitions', conn, schema='reconciled', if_exists='append', index=False)
        conn.commit()
    
    logger.info("Inserted default competition.")
    return df



def reconcile_performances(engine):
    logger.info("Reconciling performances...")

    query = """
    SELECT 
        a.athlete_name,
        a.event_clean as event_name,
        a.venue_name,
        a.result_numeric as result_value,
        a.wind_reading,
        a.pos as position_finish,
        a.competition_date,
        a.competition_level,
        a.data_source
    FROM staging.clean_world_athletics a
    WHERE a.result_numeric IS NOT NULL
      AND a.athlete_name IS NOT NULL
      AND a.event_clean IS NOT NULL
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    # Date processing
    df['month'] = pd.to_datetime(df['competition_date'], errors='coerce').dt.month
    df['month_name'] = df['month'].apply(lambda x: calendar.month_name[int(x)] if not pd.isna(x) else 'Unknown')
    df['athlete_name_clean'] = df['athlete_name'].str.strip().str.title()
    df['event_name_clean'] = df['event_name']
    df['venue_name_clean'] = df['venue_name'].str.strip().str.title()

    with engine.connect() as conn:
        athletes = pd.read_sql(text("SELECT athlete_id, athlete_name_clean FROM reconciled.athletes"), conn)
        events = pd.read_sql(text("SELECT event_id, event_name_standardized FROM reconciled.events"), conn)
        venues = pd.read_sql(text("SELECT venue_id, venue_name_clean, city_name FROM reconciled.venues"), conn)
        weather = pd.read_sql(text("SELECT weather_id, venue_name, month_name FROM reconciled.weather_conditions"), conn)

    logger.info(f"Reference table counts - Athletes: {len(athletes)}, Events: {len(events)}, Venues: {len(venues)}")

    df = df.merge(athletes, on='athlete_name_clean', how='left')
    df = df.merge(events, left_on='event_name_clean', right_on='event_name_standardized', how='left')
    df = df.merge(venues, on='venue_name_clean', how='left')
    
    # Weather matching - use venue city if available
    df = df.merge(weather, left_on=['venue_name', 'month_name'], right_on=['venue_name', 'month_name'], how='left')
    
    # Assign all performances to the single default competition
    df['competition_id'] = 1

    # Data quality checks
    logger.info(f"Before filtering: {len(df)} records")
    
    # Keep records with essential IDs
    df = df.dropna(subset=['athlete_id', 'event_id', 'competition_id'])
    logger.info(f"After requiring athlete/event/competition IDs: {len(df)} records")
    
    # Fill missing foreign keys with default values
    df['venue_id'] = df['venue_id'].fillna(1)  # Default venue
    df['weather_id'] = df['weather_id'].fillna(1)  # Default weather
    df['data_quality_score'] = 8

    # Fix 6: Select final columns
    final = df[['athlete_id', 'event_id', 'venue_id', 'weather_id', 'competition_id',
                'competition_date', 'result_value', 'wind_reading', 'position_finish',
                'data_source', 'data_quality_score']]

    logger.info(f"Final performance records: {len(final)}")

    # Fix 7: Proper save with SQLAlchemy 2.0
    with engine.connect() as conn:
        final.to_sql('performances', conn, schema='reconciled', if_exists='append', index=False)
        conn.commit()
    
    logger.info(f"Inserted {len(final)} performances.")
    return final



def extract_location_from_venue(venue_name):     ### for Reconciled_venues
    """Extract city and country from venue name"""
    if pd.isna(venue_name):
        return {'city': 'Unknown', 'country': 'Unknown', 'country_code': 'UNK'}
    
    venue_str = str(venue_name).strip()
    
    # Handle pattern: "Venue Name, City, State (Country)"
    # Example: "Hayward Field, Eugene, OR (USA)"
    if ',' in venue_str and '(' in venue_str:
        parts = venue_str.split(',')
        if len(parts) >= 2:
            # Get city (second part)
            city = parts[1].strip()
            
            # Extract country from parentheses
            if '(' in venue_str and ')' in venue_str:
                country_part = venue_str[venue_str.find('(')+1:venue_str.find(')')]
                country = country_part.strip()
                country_code = country[:3].upper()  # USA, GBR, etc.
            else:
                country = 'Unknown'
                country_code = 'UNK'
                
            return {'city': city, 'country': country, 'country_code': country_code}
    
    # Handle pattern: "Venue Name, City"
    # Example: "Olympic Stadium, London"
    elif ',' in venue_str:
        parts = venue_str.split(',')
        city = parts[-1].strip()  # Last part is usually the city
        return {'city': city, 'country': 'Unknown', 'country_code': 'UNK'}
    
    # Handle pattern with parentheses but no comma
    # Example: "Wembley Stadium (GBR)"
    elif '(' in venue_str and ')' in venue_str:
        country_part = venue_str[venue_str.find('(')+1:venue_str.find(')')]
        country = country_part.strip()
        country_code = country[:3].upper()
        
        # Try to extract city from venue name (remove venue type)
        venue_base = venue_str[:venue_str.find('(')].strip()
        city_words = ['stadium', 'field', 'arena', 'track', 'centre', 'center']
        city = venue_base
        for word in city_words:
            city = city.replace(word, '').replace(word.title(), '').strip()
        
        return {'city': city if city else 'Unknown', 'country': country, 'country_code': country_code}
    
    # Fallback: use venue name as city
    return {'city': venue_str, 'country': 'Unknown', 'country_code': 'UNK'}



def clear_reconciled_tables(engine):
    """Clear existing data before re-loading"""
    tables = ['performances', 'weather_conditions', 'competitions', 'venues', 'events', 'athletes']
    
    with engine.connect() as conn:
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE reconciled.{table} RESTART IDENTITY CASCADE"))
            logger.info(f"Cleared reconciled.{table}")
        conn.commit()



def main():
    try:
        logger.info("Starting reconciled data layer creation...")
        engine = create_db_connection()

        # Clear existing data first
        clear_reconciled_tables(engine)

        reconcile_athletes(engine)
        reconcile_events(engine)
        reconcile_venues(engine)
        reconcile_weather(engine)
        reconcile_competitions(engine)
        reconcile_performances(engine)

        # Fix the count queries
        with engine.connect() as conn:
            tables = ['athletes', 'events', 'venues', 'weather_conditions', 'competitions', 'performances']
            for table in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM reconciled.{table}")).scalar()
                logger.info(f"reconciled.{table}: {count} records")

        logger.info("Reconciled data layer created successfully.")
    except Exception as e:
        logger.error(f"Reconciled data layer creation failed: {e}")
        raise

if __name__ == "__main__":
    main()
