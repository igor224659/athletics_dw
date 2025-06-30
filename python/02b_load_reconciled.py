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



# def reconcile_venues(engine):
#     """Create reconciled venue dimension with stadium-to-city soft matching"""
#     logger.info("Reconciling venue data with soft city matching...")

#     query = """
#     WITH filtered_cities AS (
#         SELECT * 
#         FROM staging.clean_cities 
#         WHERE population > 20000
#     )
#     SELECT DISTINCT 
#         a.venue_name,
#         c.city_name,
#         c.country_name,
#         c.latitude,
#         c.longitude,
#         c.population,
#         c.altitude_category,
#         c.data_source
#     FROM staging.clean_world_athletics a
#     LEFT JOIN filtered_cities c 
#         ON LOWER(a.venue_name) LIKE '%' || LOWER(c.city_name) || '%'
#     WHERE a.venue_name IS NOT NULL
#     """

#     with engine.connect() as conn:
#         venues_df = pd.read_sql(text(query), conn)

#     # Clean and enrich
#     venues_df['venue_name_clean'] = venues_df['venue_name'].str.strip().str.title()
#     venues_df['city_name'] = venues_df['city_name'].fillna('Unknown')
#     venues_df['country_name'] = venues_df['country_name'].fillna('Unknown')

#     # Deduplicate: prefer match with valid latitude
#     venues_df['lat_score'] = venues_df['latitude'].apply(lambda x: 1 if pd.notna(x) and x != 0 else 0)
#     venues_df = venues_df.sort_values(by='lat_score', ascending=False)
#     venues_df = venues_df.drop_duplicates(subset='venue_name_clean', keep='first')
#     venues_df.drop(columns='lat_score', inplace=True)

#     # Altitude & climate
#     def estimate_altitude(lat):
#         if pd.isna(lat): return 100
#         return max(0, int(abs(lat) * 50))

#     def determine_climate(lat):
#         if pd.isna(lat): return 'Unknown'
#         abs_lat = abs(lat)
#         if abs_lat < 23.5: return 'Tropical'
#         elif abs_lat < 40: return 'Subtropical'
#         elif abs_lat < 60: return 'Temperate'
#         else: return 'Polar'

#     venues_df['altitude'] = venues_df['latitude'].apply(estimate_altitude)
#     venues_df['altitude_category'] = venues_df['altitude'].apply(
#         lambda x: 'High' if x > 1500 else 'Moderate' if x > 500 else 'Sea Level'
#     )
#     venues_df['climate_zone'] = venues_df['latitude'].apply(determine_climate)
#     venues_df['data_quality_score'] = venues_df['latitude'].apply(lambda lat: 8 if lat else 6)
#     venues_df['geographic_source'] = 'Stadium-to-City LIKE Match'

#     # Save to reconciled layer
#     with engine.connect() as conn:
#         venues_df.to_sql('venues', conn, schema='reconciled', if_exists='replace', index=False)
#         conn.commit()
    
#     logger.info(f"Reconciled {len(venues_df)} venues with best available geographic match")

#     return venues_df



def reconcile_venues(engine):
    logger.info("Reconciling venues with improved city matching...")
    
    # Step 1: Get venues and extract cities using Python (FAST)
    venues_query = """
    SELECT DISTINCT venue_name
    FROM staging.clean_world_athletics
    WHERE venue_name IS NOT NULL
    """
    
    with engine.connect() as conn:
        venues_df = pd.read_sql(text(venues_query), conn)
    
    # Step 2: Extract city names
    location_info = venues_df['venue_name'].apply(extract_location_from_venue)
    venues_df['city_extracted'] = [info['city'] for info in location_info]
    venues_df['country_extracted'] = [info['country'] for info in location_info]
    
    # Step 3: Get filtered cities (FAST)
    cities_query = """
    SELECT city_name, country_name, latitude, longitude, population, altitude_category
    FROM staging.clean_cities
    
    """
    
    with engine.connect() as conn:
        cities_df = pd.read_sql(text(cities_query), conn)
    
    # Step 4: Exact matching on extracted city names (FAST)
    cities_df['city_upper'] = cities_df['city_name'].str.upper()
    venues_df['city_upper'] = venues_df['city_extracted'].str.upper()
    
    merged_df = venues_df.merge(cities_df, on='city_upper', how='left')
    
    # Step 5: Your excellent deduplication logic
    merged_df['venue_name_clean'] = merged_df['venue_name'].str.strip().str.title()
    merged_df['lat_score'] = merged_df['latitude'].apply(lambda x: 1 if pd.notna(x) and x != 0 else 0)
    merged_df = merged_df.sort_values(by='lat_score', ascending=False)
    merged_df = merged_df.drop_duplicates(subset='venue_name_clean', keep='first')

    # Altitude & climate
    def estimate_altitude(lat):
        if pd.isna(lat): return 100
        return max(0, int(abs(lat) * 50))

    def determine_climate(lat):
        if pd.isna(lat): return 'Unknown'
        abs_lat = abs(lat)
        if abs_lat < 23.5: return 'Tropical'
        elif abs_lat < 40: return 'Subtropical'
        elif abs_lat < 60: return 'Temperate'
        else: return 'Polar'

    merged_df['altitude'] = merged_df['latitude'].apply(estimate_altitude)
    merged_df['altitude_category'] = merged_df['altitude'].apply(
        lambda x: 'High' if x > 1500 else 'Moderate' if x > 500 else 'Sea Level'
    )
    merged_df['climate_zone'] = merged_df['latitude'].apply(determine_climate)
    merged_df['data_quality_score'] = merged_df['latitude'].apply(lambda lat: 8 if lat else 6)
    merged_df['geographic_source'] = 'Stadium-to-City Match'


    # Keep only specific columns
    merged_df = merged_df[['venue_name_clean', 'city_upper', 'country_extracted', 'latitude', 'longitude', 'altitude', 'altitude_category', 'climate_zone', 'population',
                           'data_quality_score', 'geographic_source']]

    # Save to reconciled layer
    with engine.connect() as conn:
        merged_df.to_sql('venues', conn, schema='reconciled', if_exists='replace', index=False)
        conn.commit()
    
    logger.info(f"Reconciled {len(merged_df)} venues with best available geographic match")

    return merged_df



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

    logger.info(f"STEP 1 - Original performance data: {len(df)} records")

    # Extract month from performance date for weather matching
    df['competition_date_parsed'] = pd.to_datetime(df['competition_date'], errors='coerce')
    df['month'] = df['competition_date_parsed'].dt.month
    df['month_name'] = df['month'].apply(lambda x: calendar.month_name[int(x)] if not pd.isna(x) else 'Unknown')
    
    # Clean names for matching
    df['athlete_name_clean'] = df['athlete_name'].str.strip().str.title()
    df['event_name_clean'] = df['event_name']
    df['venue_name_clean'] = df['venue_name'].str.strip().str.title()

    # Read reference tables
    with engine.connect() as conn:
        athletes = pd.read_sql(text("SELECT athlete_id, athlete_name_clean FROM reconciled.athletes"), conn)
        events = pd.read_sql(text("SELECT event_id, event_name_standardized FROM reconciled.events"), conn)
        venues = pd.read_sql(text("SELECT venue_id, venue_name_clean, city_name FROM reconciled.venues"), conn)
        weather = pd.read_sql(text("SELECT weather_id, venue_name as city_name, month_name FROM reconciled.weather_conditions"), conn)

    logger.info(f"Reference table counts - Athletes: {len(athletes)}, Events: {len(events)}, Venues: {len(venues)}")

    # Step-by-step joins with diagnostics
    df = df.merge(athletes, on='athlete_name_clean', how='left')
    logger.info(f"STEP 2 - After athlete join: {len(df)} records")

    df = df.merge(events, left_on='event_name_clean', right_on='event_name_standardized', how='left')
    logger.info(f"STEP 3 - After event join: {len(df)} records")


    # Before venue join, deduplicate venues by name (keep first)
    venues_dedup = venues.drop_duplicates(subset=['venue_name_clean'], keep='first')
    logger.info(f"Venues after deduplication: {len(venues_dedup)} (was {len(venues)})")
    # Use deduplicated venues for join
    df = df.merge(venues_dedup, on='venue_name_clean', how='left')
    logger.info(f"STEP 4 - After venue join: {len(df)} records")


    # FIXED WEATHER JOIN: Match city from venue + month from performance date
    df = df.merge(weather, on=['city_name', 'month_name'], how='left')
    logger.info(f"STEP 5 - After weather join: {len(df)} records")
    
    # Assign all performances to the single default competition
    df['competition_id'] = 1

    # Data quality filtering
    logger.info(f"Before filtering: {len(df)} records")
    df = df.dropna(subset=['athlete_id', 'event_id'])
    logger.info(f"After requiring athlete/event IDs: {len(df)} records")
    
    # Fill missing foreign keys with default values
    df['venue_id'] = df['venue_id'].fillna(1)  # Default venue
    df['weather_id'] = df['weather_id'].fillna(1)  # Default weather
    df['data_quality_score'] = 8

    # Select final columns
    final = df[['athlete_id', 'event_id', 'venue_id', 'weather_id', 'competition_id',
                'competition_date', 'result_value', 'wind_reading', 'position_finish',
                'data_source', 'data_quality_score']]

    # Convert data types
    final['athlete_id'] = final['athlete_id'].astype(int)
    final['event_id'] = final['event_id'].astype(int) 
    final['venue_id'] = final['venue_id'].astype(int)
    final['weather_id'] = final['weather_id'].astype(int)
    final['competition_id'] = final['competition_id'].astype(int)

    logger.info(f"Final performance records ready for insert: {len(final)}")
    logger.info(f"Weather match success: {(final['weather_id'] != 1).sum()}/{len(final)} performances have weather data")

    # Use chunked save for large dataset
    logger.info("Starting chunked save of performance data...")
    chunked_save_to_postgres(final, 'performances', engine, schema='reconciled', chunk_size=25000)
    
    logger.info(f"Inserted {len(final)} performances.")
    return final



def chunked_save_to_postgres(df, table_name, engine, schema='reconciled', chunk_size=25000):
    """Save large DataFrame in chunks"""
    logger.info(f"Saving {len(df)} records to {schema}.{table_name} in chunks of {chunk_size}...")
    
    total_chunks = len(df) // chunk_size + 1
    
    for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
        chunk_end = min(chunk_start + chunk_size, len(df))
        chunk = df.iloc[chunk_start:chunk_end]
        
        logger.info(f"Saving chunk {i+1}/{total_chunks} ({chunk_start}:{chunk_end})")
        
        if_exists_param = 'replace' if i == 0 else 'append'
        
        with engine.connect() as conn:
            chunk.to_sql(table_name, conn, schema=schema, 
                        if_exists=if_exists_param, index=False, method='multi')
            conn.commit()
    
    logger.info(f"✓ {table_name} saved successfully")



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
