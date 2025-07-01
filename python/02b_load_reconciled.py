import io
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



def reconcile_venues(engine):
    logger.info("Reconciling venues...")
    
    # Get venues and extract cities using Python
    venues_query = """
    SELECT DISTINCT venue_name
    FROM staging.clean_world_athletics
    WHERE venue_name IS NOT NULL
    """
    
    with engine.connect() as conn:
        venues_df = pd.read_sql(text(venues_query), conn)
    
    # Extract city names
    location_info = venues_df['venue_name'].apply(extract_location_from_venue)
    venues_df['city_extracted'] = [info['city'] for info in location_info]
    venues_df['country_extracted'] = [info['country'] for info in location_info]
    
    # Get filtered cities
    cities_query = """
    SELECT city_name, country_name, latitude, longitude, altitude, altitude_category
    FROM staging.clean_cities
    WHERE altitude IS NOT NULL
    """
    
    with engine.connect() as conn:
        cities_df = pd.read_sql(text(cities_query), conn)
    
    # Prepare for matching
    venues_df['city_extracted_clean'] = venues_df['city_extracted'].str.strip().str.upper()
    venues_df['country_extracted_clean'] = venues_df['country_extracted'].str.strip().str.upper()
    
    # Match venues to cities
    merged_df = venues_df.merge(
        cities_df, 
        left_on=['city_extracted_clean', 'country_extracted_clean'], 
        right_on=['city_name', 'country_name'], 
        how='left'
    )
    
    # Create clean venue data
    merged_df['venue_name_clean'] = merged_df['venue_name'].str.strip().str.title()
    merged_df['city_name'] = merged_df['city_name'].fillna(merged_df['city_extracted']).str.title()
    merged_df['country_name'] = merged_df['country_name'].fillna(merged_df['country_extracted']).str.upper()
    merged_df = merged_df.drop_duplicates(subset='venue_name_clean', keep='first')

    # Handle missing data with defaults
    #merged_df['altitude'] = merged_df['altitude'].fillna(100)  # Default for unmatched venues
    #merged_df['altitude_category'] = merged_df['altitude_category'].fillna('Sea Level') 

    # Climate categorization
    def determine_climate(lat):
        if pd.isna(lat): return 'Unknown'
        abs_lat = abs(lat)
        if abs_lat < 23.5: return 'Tropical'
        elif abs_lat < 40: return 'Subtropical'
        elif abs_lat < 60: return 'Temperate'
        else: return 'Polar'

    #merged_df['latitude'] = merged_df['latitude'].fillna(0.0)
    #merged_df['longitude'] = merged_df['longitude'].fillna(0.0)
    merged_df['climate_zone'] = merged_df['latitude'].apply(determine_climate)
    merged_df['geographic_source'] = 'Venue_Parsing_Plus_GeoNames_Elevation'

    # Data quality scoring
    merged_df['data_quality_score'] = merged_df.apply(
        lambda row: 9 if (pd.notna(row['latitude']) and row['latitude'] != 0 and pd.notna(row['altitude']) and row['altitude'] > 0) 
                   else 7 if (pd.notna(row['latitude']) and row['latitude'] != 0)
                   else 5, axis=1
    )

    # Select final columns
    final_venues = merged_df[[
        'venue_name', 'venue_name_clean', 'city_name', 'country_name',
        'latitude', 'longitude', 'altitude', 'altitude_category', 
        'climate_zone', 'data_quality_score', 'geographic_source'
    ]]

    # Save to reconciled layer
    with engine.connect() as conn:
        final_venues.to_sql('venues', conn, schema='reconciled', if_exists='append', index=False)
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
    # Add created_date column to match SQL table structure
    df['created_date'] = pd.Timestamp.now()

    # Select final columns
    final = df[['athlete_id', 'event_id', 'venue_id', 'weather_id', 'competition_id',
                'competition_date', 'result_value', 'wind_reading', 'position_finish',
                'data_source', 'data_quality_score', 'created_date']]

    # Convert data types
    final['athlete_id'] = final['athlete_id'].astype(int)
    final['event_id'] = final['event_id'].astype(int) 
    final['venue_id'] = final['venue_id'].astype(int)
    final['weather_id'] = final['weather_id'].astype(int)
    final['competition_id'] = final['competition_id'].astype(int)

    logger.info(f"Final performance records ready for insert: {len(final)}")
    logger.info(f"Weather match success: {(final['weather_id'] != 1).sum()}/{len(final)} performances have weather data")

    #Use TRUNCATE + chunked append to preserve table structure
    with engine.connect() as conn:
        # Clear existing data but keep table structure
        conn.execute(text("TRUNCATE TABLE reconciled.performances RESTART IDENTITY"))
        conn.commit()
        logger.info("Cleared existing performance data")
    
    # Use modified chunked save that only appends
    logger.info("Starting save of performance data...")
    ultra_fast_postgres_append(final, 'performances', engine)
    #chunked_append_to_postgres(final, 'performances', engine, schema='reconciled', chunk_size=1000)
    
    logger.info(f"Inserted {len(final)} performances.")
    return final



def ultra_fast_postgres_append(df, table_name, engine, schema='reconciled'):
    """Ultra-fast append using PostgreSQL COPY - no table recreation"""
    logger.info(f"Ultra-fast appending {len(df)} records to {schema}.{table_name}...")
    
    # Step 1: Verify table exists
    with engine.connect() as conn:
        try:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name} LIMIT 1"))
            logger.info(f"Table {schema}.{table_name} exists and is accessible")
        except Exception as e:
            logger.error(f"Cannot access table {schema}.{table_name}: {e}")
            raise
    
    # Step 2: Prepare data for COPY
    output = io.StringIO()
    
    # Convert DataFrame to tab-separated values
    df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N', 
              date_format='%Y-%m-%d', float_format='%.6f')
    output.seek(0)
    
    # Step 3: Use PostgreSQL COPY command with proper connection handling
    with engine.begin() as conn:  # Use begin() for transaction
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()
        
        try:
            # COPY command - use fully qualified table name
            copy_sql = f"COPY {schema}.{table_name} ({','.join(df.columns)}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
            
            logger.info(f"Executing COPY command for {schema}.{table_name}")
            cursor.copy_expert(copy_sql, output)
            
            logger.info(f"{len(df)} records appended successfully using COPY!")
            
        except Exception as e:
            logger.error(f"COPY failed: {e}")
            logger.error(f"Attempted COPY command: {copy_sql}")
            raise



def extract_location_from_venue(venue_name):     ### for Reconciled_venues
    """Extract city and country from venue name"""
    if pd.isna(venue_name):
        return {'city': 'Unknown', 'country': 'Unknown', 'country_code': 'UNK'}
    
    venue_str = str(venue_name).strip()

    # Handle pattern: "Stadium Name, City (Country)"
    # Example: "Stadio Olimpico, Roma (ITA)"
    if ',' in venue_str and '(' in venue_str:
        parts = venue_str.split(',')
        if len(parts) >= 2:
            # Get city part (after comma, before parentheses)
            city_part = parts[1].strip()
            
            # Extract city name (remove country in parentheses)
            if '(' in city_part:
                city = city_part[:city_part.find('(')].strip()
                
                # Extract country from parentheses
                country_start = venue_str.find('(')
                country_end = venue_str.find(')')
                if country_start != -1 and country_end != -1:
                    country = venue_str[country_start+1:country_end].strip()
                else:
                    country = 'Unknown'
            else:
                city = city_part
                country = 'Unknown'
                
            return {'city': city.upper(), 'country': country.upper(), 'country_code': country.upper()[:3]}
    
    # Handle pattern: "City (Country)" - no stadium name
    # Example: "Rieti (ITA)"
    elif '(' in venue_str and ')' in venue_str and ',' not in venue_str:
        city_part = venue_str[:venue_str.find('(')].strip()
        
        country_start = venue_str.find('(')
        country_end = venue_str.find(')')
        country = venue_str[country_start+1:country_end].strip()
        
        return {'city': city_part.upper(), 'country': country.upper(), 'country_code': country.upper()[:3]}
    
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
