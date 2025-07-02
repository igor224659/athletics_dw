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
        elif 'throw' in e or 'put' in e or 'vault' in e:
            return 'Throws'
        # Check distances AFTER hurdles
        elif any(x in e for x in ['100 metres', '200 metres', '300 metres', '400 metres', '60 metres']):
            return 'Sprint'
        elif any(x in e for x in ['kilometres race walk', 'kilometres']):
            return 'Distance (Road)'
        elif any(x in e for x in ['600', '800', '1000', '1500', '2000', '3000', '5000', '10000', 'marathon', 'mile', 'metres race walk']):
            return 'Distance'
        else:
            return 'Other'

    def extract_distance(event):
        """
        Extract distance in meters from event name, handling various formats
        """
        if not event:
            return None
        
        import re
        event_lower = event.lower()
        
        # Word-to-number mapping for written distances
        word_to_number = {
            'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
            'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
            'half': 0.5
        }
        
        # 1. Handle Mile distances (convert to meters)
        mile_patterns = [
            r'(\d+(?:\.\d+)?)\s*mile',  # "1.5 mile", "1 mile"
            r'(one|two|three|four|five|half)\s+mile',  # "One Mile", "Half Mile"
            r'(\d+)\s*mi\b'  # "1 mi"
        ]
        
        for pattern in mile_patterns:
            match = re.search(pattern, event_lower)
            if match:
                distance_str = match.group(1)
                if distance_str in word_to_number:
                    distance_miles = word_to_number[distance_str]
                else:
                    try:
                        distance_miles = float(distance_str)
                    except ValueError:
                        continue
                # Convert miles to meters (1 mile = 1609.344 meters)
                return int(distance_miles * 1609.344)
        
        # 2. Handle meter distances with various formats
        meter_patterns = [
            r'(\d+)\s*(?:metres|meters|m)\b',  # "100 metres", "200m", "400 meters"
            r'(\d+)\s*(?=m\s)',  # "100m " (with space after m)
            r'(\d+)m(?=\s|$)',   # "100m" at end or followed by space
        ]
        
        for pattern in meter_patterns:
            match = re.search(pattern, event_lower)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        
        # 3. Handle kilometer distances (convert to meters)
        km_patterns = [
            r'(\d+(?:\.\d+)?)\s*(?:kilometres|kilometers|km)\b',
        ]
        
        for pattern in km_patterns:
            match = re.search(pattern, event_lower)
            if match:
                try:
                    distance_km = float(match.group(1))
                    return int(distance_km * 1000)  # Convert km to meters
                except ValueError:
                    continue
        
        # 4. Handle special cases
        special_distances = {
            'marathon': 42195,  # Marathon is 42.195 km
            'half marathon': 21098,  # Half marathon
            'steeplechase': 3000,  # Standard steeplechase
            '110m hurdles': 110,
            '100m hurdles': 100,
            '400m hurdles': 400,
        }
        
        for key, distance in special_distances.items():
            if key in event_lower:
                # Check if there's a specific distance mentioned with steeplechase
                if 'steeplechase' in key:
                    steeple_match = re.search(r'(\d+)(?:m|metres|meters)?\s*steeplechase', event_lower)
                    if steeple_match:
                        try:
                            return int(steeple_match.group(1))
                        except ValueError:
                            pass
                return distance
        
        # 5. Handle relay distances (extract the individual leg distance)
        relay_match = re.search(r'(\d+)(?:m|metres|meters)?\s*(?:x\s*)?(\d+)(?:m|metres|meters)?.*relay', event_lower)
        if relay_match:
            try:
                leg_distance = int(relay_match.group(2)) if relay_match.group(2) else int(relay_match.group(1))
                return leg_distance
            except ValueError:
                pass
        
        return None

    df['event_name_standardized'] = df['event_name']
    df['event_group'] = df['event_name'].apply(categorize)
    
    # Categorization logic
    def get_category(group):
        if group in ['Sprint', 'Distance', 'Hurdles']:
            return 'Track'
        elif group in ['Throws', 'Jumps']:
            return 'Field'
        elif group in ['Distance (Road)']:
            return 'Road'
        else:
            return 'Multi-event'
    
    df['event_category'] = df['event_group'].apply(get_category)
    df['is_outdoor_event'] = True
    
    # Measurement unit logic
    def get_measurement_unit(category):
        if category == 'Track' or category == 'Road':
            return 'seconds'
        else:
            return 'meters'
    
    df['measurement_unit'] = df['event_category'].apply(get_measurement_unit)
    df['distance_meters'] = df['event_name'].apply(extract_distance)
    
    df['gender'] = 'Mixed' 
    
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
    """Simplified version without fuzzy matching dependencies"""
    logger.info("Reconciling venues...")
    
    # Country code mappings (3-letter to 2-letter)
    country_mapping = {
        'ITA': 'IT', 'FRA': 'FR', 'GER': 'DE', 'GBR': 'GB', 'USA': 'US',
        'SUI': 'CH', 'NOR': 'NO', 'FIN': 'FI', 'BEL': 'BE', 'RUS': 'RU',
        'ESP': 'ES', 'NED': 'NL', 'AUT': 'AT', 'SWE': 'SE', 'POL': 'PL',
        'CZE': 'CZ', 'DEN': 'DK', 'POR': 'PT', 'GRE': 'GR', 'HUN': 'HU',
        'CRO': 'HR', 'UKR': 'UA', 'CAN': 'CA', 'AUS': 'AU', 'JPN': 'JP'
    }
    
    # City name mappings for common variations
    city_mapping = {
        'ROMA': 'ROME', 'MÜNCHEN': 'MUNICH', 'WIEN': 'VIENNA',
        'FIRENZE': 'FLORENCE', 'VENEZIA': 'VENICE', 'NAPOLI': 'NAPLES',
        'TORINO': 'TURIN', 'BRUXELLES': 'BRUSSELS', 'ZÜRICH': 'ZURICH',
        'KÖLN': 'COLOGNE', 'LISBOA': 'LISBON'
    }
    
    # Get venues and extract cities using improved Python logic
    venues_query = """
    SELECT DISTINCT venue_name
    FROM staging.clean_world_athletics
    WHERE venue_name IS NOT NULL
    """
    
    with engine.connect() as conn:
        venues_df = pd.read_sql(text(venues_query), conn)
    
    # Extract location info with improved logic
    def extract_location_improved(venue_name):
        if pd.isna(venue_name):
            return {'city': 'Unknown', 'country_2': 'XX'}
        
        venue_str = str(venue_name).strip()
        
        # Extract country code from parentheses
        import re
        country_match = re.search(r'\(([A-Z]{2,3})\)', venue_str)
        country_3 = country_match.group(1) if country_match else 'Unknown'
        country_2 = country_mapping.get(country_3, country_3[:2] if len(country_3) >= 2 else 'XX')
        
        # Remove country part for city extraction
        venue_clean = re.sub(r'\s*\([^)]+\)', '', venue_str).strip()
        
        # Extract city
        if ',' in venue_clean:
            parts = venue_clean.split(',')
            # Get the last meaningful part (skip state abbreviations)
            for part in reversed(parts):
                part = part.strip()
                if len(part) > 2 and not re.match(r'^[A-Z]{2}$', part):
                    city = part
                    break
            else:
                city = parts[-1].strip()
        else:
            city = venue_clean
            # Remove stadium words
            stadium_words = ['stadium', 'stadion', 'stadio', 'field', 'arena', 'track']
            for word in stadium_words:
                city = re.sub(rf'\b{word}\b', '', city, flags=re.IGNORECASE).strip()
        
        # Apply city name mapping
        city_upper = city.upper()
        city_mapped = city_mapping.get(city_upper, city_upper)
        
        return {'city': city_mapped, 'country_2': country_2}
    
    location_info = venues_df['venue_name'].apply(extract_location_improved)
    venues_df['city_extracted'] = [info['city'] for info in location_info]
    venues_df['country_extracted'] = [info['country_2'] for info in location_info]
    
    # Get cities from database
    cities_query = """
    SELECT city_name, country_name, latitude, longitude, altitude, altitude_category
    FROM staging.clean_cities
    WHERE altitude IS NOT NULL
    """
    
    with engine.connect() as conn:
        cities_df = pd.read_sql(text(cities_query), conn)
    
    logger.info(f"Found {len(venues_df)} venues and {len(cities_df)} cities")
    
    # Prepare for matching with case normalization
    venues_df['city_clean'] = venues_df['city_extracted'].str.strip().str.upper()
    venues_df['country_clean'] = venues_df['country_extracted'].str.strip().str.upper()
    cities_df['city_clean'] = cities_df['city_name'].str.strip().str.upper()
    cities_df['country_clean'] = cities_df['country_name'].str.strip().str.upper()
    
    # Perform the merge
    merged_df = venues_df.merge(
        cities_df,
        left_on=['city_clean', 'country_clean'],
        right_on=['city_clean', 'country_clean'],
        how='left'
    )
    
    # Clean up the results
    merged_df['venue_name_clean'] = merged_df['venue_name'].str.strip().str.title()
    merged_df['city_name'] = merged_df['city_name'].fillna(merged_df['city_extracted']).str.title()
    merged_df['country_name'] = merged_df['country_name'].fillna(merged_df['country_extracted']).str.upper()
    merged_df['country_code'] = merged_df['country_name'].fillna(merged_df['country_extracted']).str.upper()
    
    # Remove duplicates
    merged_df = merged_df.drop_duplicates(subset='venue_name', keep='first')
    
    # Climate categorization
    def determine_climate(lat):
        if pd.isna(lat): return 'Unknown'
        abs_lat = abs(lat)
        if abs_lat < 23.5: return 'Tropical'
        elif abs_lat < 40: return 'Subtropical'
        elif abs_lat < 60: return 'Temperate'
        else: return 'Polar'
    
    merged_df['climate_zone'] = merged_df['latitude'].apply(determine_climate)
    merged_df['geographic_source'] = 'Venue_Parsing_Plus_GeoNames_Elevation'
    
    # Data quality scoring
    merged_df['data_quality_score'] = merged_df.apply(
        lambda row: 9 if (pd.notna(row['latitude']) and pd.notna(row['altitude'])) 
                   else 7 if pd.notna(row['latitude'])
                   else 5, axis=1
    )
    
    # Report statistics
    total_venues = len(merged_df)
    matched_venues = len(merged_df[pd.notna(merged_df['latitude'])])
    logger.info(f"Matching results: {matched_venues}/{total_venues} venues matched ({matched_venues/total_venues*100:.1f}%)")
    
    # Select final columns
    final_venues = merged_df[[
        'venue_name', 'venue_name_clean', 'city_name', 'country_name', 'country_code',
        'latitude', 'longitude', 'altitude', 'altitude_category', 
        'climate_zone', 'data_quality_score', 'geographic_source'
    ]]
    
    # Save to database
    with engine.connect() as conn:
        final_venues.to_sql('venues', conn, schema='reconciled', if_exists='append', index=False)
        conn.commit()
    
    logger.info(f"Inserted {len(final_venues)} venue records")
    return final_venues



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

        # Count queries
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
