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


    # STEP 1: Normalize athlete names for deduplication
    def normalize_athlete_name(name):
        """Normalize athlete names to catch duplicates"""
        if pd.isna(name):
            return None
        
        # Convert to uppercase and strip whitespace
        normalized = str(name).upper().strip()
        
        # Remove multiple spaces
        normalized = ' '.join(normalized.split())
        
        # Remove common punctuation variations
        normalized = normalized.replace('.', '').replace(',', '').replace("'", '')
        
        # Remove common suffixes/titles
        suffixes_to_remove = [' JR', ' SR', ' III', ' II', ' JUNIOR', ' SENIOR']
        for suffix in suffixes_to_remove:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)].strip()
        
        return normalized
    
    # Apply normalization
    df['athlete_name_normalized'] = df['athlete_name'].apply(normalize_athlete_name)
    
    # STEP 2: Deduplicate athletes
    # Sort to ensure we keep the most complete record (by nationality, then by source)
    df_sorted = df.sort_values(
        by=['athlete_name_normalized', 'nationality', 'data_source'],
        na_position='last'
    )
    
    # Keep first occurrence of each normalized name
    df_dedup = df_sorted.drop_duplicates(subset=['athlete_name_normalized'], keep='first').copy()
    
    logger.info(f"Deduplication: {len(df)} → {len(df_dedup)} athletes ({len(df) - len(df_dedup)} duplicates removed)")
    

    # STEP 3: Prepare final data
    df_dedup['athlete_name_clean'] = df_dedup['athlete_name'].str.strip().str.title()
    df_dedup['nationality_standardized'] = df_dedup['nationality'].str.strip().str.title()
    df_dedup['birth_decade'] = 'Unknown'
    df_dedup['specialization'] = 'All-around'
    df_dedup['data_quality_score'] = 8
    df_dedup['source_system'] = df_dedup['data_source']
    df_dedup['nationality_code'] = df_dedup['nationality'].str.upper().str[:3]

    # Normalize gender
    def normalize_gender(gender_val):
        if pd.isna(gender_val):
            return 'U'
        gender_str = str(gender_val).lower().strip()
        if gender_str in ['female', 'f', 'w']:
            return 'F'
        elif gender_str in ['male', 'm']:
            return 'M'
        else:
            return 'U'
    
    df_dedup['gender_normalized'] = df_dedup['gender'].apply(normalize_gender)

    # Select final columns
    final = df_dedup[['athlete_name', 'athlete_name_clean', 'nationality_standardized', 
                      'nationality_code', 'gender_normalized', 'birth_decade', 'specialization',
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


    # Remove multi-events (Decathlon, Heptathlon)
    logger.info("Filtering out multi-events...")
    initial_count = len(df)

    df = df[~df['event_name'].str.contains('(?i)(?:decathlon|heptathlon)', na=False)]

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} multi-event records")


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
    

    # Assign specific genders to hurdles events
    def assign_gender(event_name):
        if pd.isna(event_name):
            return 'Mixed'
    
        event_str = str(event_name).lower()
    
        if '110' in event_str and 'hurdles' in event_str or '30 Kilometres Race Walk' in event_str:
            return 'M'  # Men's 110m Hurdles
        elif '100' in event_str and 'hurdles' in event_str:
            return 'F'  # Women's 100m Hurdles
        else:
            return 'Mixed'  # All other events

    df['gender'] = df['event_name'].apply(assign_gender)    
    

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
    """
    Reconcile weather data - use existing temperature classifications from transform stage
    """
    logger.info("Reconciling weather conditions...")

    # Get existing temperature data (already processed in transform stage)
    query = """
    SELECT DISTINCT
        "City" as venue_name,   
        "Month" as month,
        "AvgTemperature" as temperature,
        temperature_category,  -- Already created in transform stage
        data_source
    FROM staging.clean_temperature
    WHERE "City" IS NOT NULL 
      AND "Month" IS NOT NULL
      AND temperature_category IS NOT NULL
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    logger.info(f"Loaded {len(df)} temperature records from transform stage")

    # Clean up data
    df = df.dropna(subset=['venue_name', 'month'])

    # Standardize city names to match venue standardization
    def standardize_weather_city_name(city_name):
        if pd.isna(city_name):
            return 'UNKNOWN'
        
        city_str = str(city_name).strip().upper()
        
        # Apply same standardization as venues
        standardization_map = {
            'ROMA': 'ROME', 'ATHINA': 'ATHENS', 'BRUXELLES': 'BRUSSELS',
            'LA HABANA': 'HAVANA', 'ZÜRICH': 'ZURICH', 'MÜNCHEN': 'MUNICH',
            'WIEN': 'VIENNA', 'MOSKVA': 'MOSCOW', 'BUCUREŞTI': 'BUCHAREST',
            'PRAHA': 'PRAGUE', 'WARSZAWA': 'WARSAW'
        }
        
        return standardization_map.get(city_str, city_str)
    
    df['venue_name'] = df['venue_name'].apply(standardize_weather_city_name)

    # Add month names and seasons
    df['month_name'] = df['month'].apply(lambda x: calendar.month_name[int(x)] if not pd.isna(x) else 'Unknown')

    def get_season(month):
        if pd.isna(month):
            return 'Unknown'
        month = int(month)
        if month in [12, 1, 2]: return 'Winter'
        elif month in [3, 4, 5]: return 'Spring'
        elif month in [6, 7, 8]: return 'Summer'
        else: return 'Fall'
    
    df['season_category'] = df['month'].apply(get_season)
    df['has_actual_data'] = True
    df['weather_source'] = df['data_source']

    # Add weather estimates for major athletics cities missing from temperature data
    # Get list of cities from venues that need weather
    with engine.connect() as conn:
        venue_cities = pd.read_sql(text("""
            SELECT DISTINCT 
                CASE 
                    WHEN venue_name LIKE '%Sacramento%' THEN 'SACRAMENTO'
                    WHEN venue_name LIKE '%Eugene%' THEN 'EUGENE'
                    WHEN venue_name LIKE '%Austin%' THEN 'AUSTIN'
                    WHEN venue_name LIKE '%Berlin%' THEN 'BERLIN'
                    WHEN venue_name LIKE '%Monaco%' THEN 'MONACO'
                    WHEN venue_name LIKE '%Lausanne%' THEN 'LAUSANNE'
                    WHEN venue_name LIKE '%Kingston%' THEN 'KINGSTON'
                    WHEN venue_name LIKE '%Des Moines%' THEN 'DES MOINES'
                    WHEN venue_name LIKE '%Palo Alto%' THEN 'SAN FRANCISCO'
                    WHEN venue_name LIKE '%Walnut%' THEN 'LOS ANGELES'
                    WHEN venue_name LIKE '%Indianapolis%' THEN 'INDIANAPOLIS'
                    WHEN venue_name LIKE '%Gainesville%' THEN 'GAINESVILLE'
                    WHEN venue_name LIKE '%Knoxville%' THEN 'KNOXVILLE'
                    WHEN venue_name LIKE '%Doha%' THEN 'DOHA'
                    ELSE NULL
                END as needed_city
            FROM staging.clean_world_athletics
            WHERE venue_name IS NOT NULL
        """), conn)
    
    needed_cities = set(venue_cities['needed_city'].dropna().unique())
    existing_cities = set(df['venue_name'].unique())
    missing_cities = needed_cities - existing_cities
    
    if missing_cities:
        #logger.info(f"Adding weather estimates for {len(missing_cities)} missing athletics cities: {missing_cities}")
        
        # Simple climate-based estimates for missing cities
        city_climate_estimates = {
            'BERLIN': {'temps': [0, 1, 5, 9, 14, 17, 19, 19, 15, 10, 5, 2], 'climate': 'Continental'},
            'SACRAMENTO': {'temps': [10, 13, 16, 20, 25, 30, 33, 32, 28, 22, 15, 10], 'climate': 'Mediterranean'},
            'EUGENE': {'temps': [5, 7, 10, 13, 17, 21, 24, 24, 20, 15, 9, 5], 'climate': 'Temperate'},
            'AUSTIN': {'temps': [10, 13, 18, 23, 28, 32, 35, 35, 31, 25, 18, 12], 'climate': 'Subtropical'},
            'MONACO': {'temps': [9, 10, 13, 16, 20, 24, 27, 27, 23, 19, 13, 10], 'climate': 'Mediterranean'},
            'LAUSANNE': {'temps': [1, 3, 7, 11, 16, 20, 22, 21, 17, 12, 6, 2], 'climate': 'Temperate'},
            'KINGSTON': {'temps': [25, 25, 26, 27, 28, 29, 29, 29, 28, 27, 26, 25], 'climate': 'Tropical'},
            'DES MOINES': {'temps': [-5, -2, 5, 12, 18, 24, 26, 25, 20, 13, 5, -2], 'climate': 'Continental'},
            'SAN FRANCISCO': {'temps': [10, 12, 13, 15, 16, 17, 17, 18, 19, 17, 14, 11], 'climate': 'Mediterranean'},
            'LOS ANGELES': {'temps': [14, 15, 16, 18, 20, 22, 24, 25, 24, 21, 17, 14], 'climate': 'Mediterranean'},
            'INDIANAPOLIS': {'temps': [-2, 1, 7, 14, 20, 25, 27, 26, 22, 15, 8, 1], 'climate': 'Continental'},
            'GAINESVILLE': {'temps': [11, 14, 18, 22, 26, 29, 31, 31, 29, 24, 18, 13], 'climate': 'Subtropical'},
            'KNOXVILLE': {'temps': [3, 6, 11, 16, 21, 26, 28, 27, 23, 17, 11, 5], 'climate': 'Subtropical'},
            'DOHA': {'temps': [18, 20, 25, 30, 36, 41, 42, 41, 38, 32, 26, 20], 'climate': 'Desert'}
        }
        
        # Temperature categorization function (same as transform stage)
        def categorize_temperature(temp):
            if temp < 10: return 'Cold'
            elif temp < 18: return 'Cool'
            elif temp < 24: return 'Moderate'
            elif temp < 30: return 'Warm'
            else: return 'Hot'
        
        # Generate estimates for missing cities
        estimates = []
        for city in missing_cities:
            if city in city_climate_estimates:
                city_data = city_climate_estimates[city]
                temps = city_data['temps']
                climate = city_data['climate']
                
                for month_idx, temp in enumerate(temps, 1):
                    month_name = calendar.month_name[month_idx]
                    estimates.append({
                        'venue_name': city,
                        'month': month_idx,
                        'temperature': temp,
                        'temperature_category': categorize_temperature(temp),
                        'month_name': month_name,
                        'season_category': get_season(month_idx),
                        'has_actual_data': False,
                        'weather_source': f'Athletics_Estimate_{climate}'
                    })
        
        if estimates:
            estimates_df = pd.DataFrame(estimates)
            df = pd.concat([df, estimates_df], ignore_index=True)
            #logger.info(f"Added {len(estimates)} weather estimate records for {len(set(est['venue_name'] for est in estimates))} cities")

    # Select final columns
    final = df[['venue_name', 'month_name', 'temperature',
                'temperature_category', 'season_category',
                'has_actual_data', 'weather_source']]
    
    logger.info(f"Final weather data: {len(final)} records for {final['venue_name'].nunique()} cities")

    # Save to database
    with engine.connect() as conn:
       final.to_sql('weather_conditions', conn, schema='reconciled', if_exists='append', index=False)
       conn.commit()

    logger.info(f"Inserted {len(final)} weather records")
    return final



def reconcile_venues(engine):
    """
    Reconcile venues with comprehensive city extraction based on actual venue patterns
    """
    logger.info("Reconciling venues with city extraction...")
    
    def extract_location_from_venue(venue_name):
        """Extract city and country from venue name patterns"""
        if pd.isna(venue_name):
            return {'city': 'Unknown', 'country_2': 'XX'}
        
        venue_str = str(venue_name).strip()
        import re
        
        # Extract country code from parentheses
        country_match = re.search(r'\(([A-Z]{3})\)', venue_str)
        country_3 = country_match.group(1) if country_match else 'Unknown'
        
        # Map 3-letter to 2-letter country codes
        country_mapping = {
            'USA': 'US', 'GBR': 'GB', 'GER': 'DE', 'FRA': 'FR', 'ITA': 'IT',
            'SUI': 'CH', 'BEL': 'BE', 'SWE': 'SE', 'FIN': 'FI', 'GRE': 'GR',
            'CHN': 'CN', 'JAM': 'JM', 'CUB': 'CU', 'MON': 'MC', 'RUS': 'RU',
            'NED': 'NL', 'ESP': 'ES', 'JPN': 'JP', 'HUN': 'HU', 'AUT': 'AT',
            'POL': 'PL', 'CZE': 'CZ', 'BRA': 'BR', 'QAT': 'QA', 'UKR': 'UA',
            'AUS': 'AU', 'CRO': 'HR', 'ROU': 'RO', 'BUL': 'BG', 'KOR': 'KR',
            'BLR': 'BY', 'URS': 'RU', 'NOR': 'NO'
        }
        country_2 = country_mapping.get(country_3, country_3[:2] if len(country_3) >= 2 else 'XX')
        
        # PATTERN 1: "City, STATE (COUNTRY)" → Extract city, ignore state
        # Example: "Sacramento, CA (USA)" → "Sacramento"
        pattern1 = re.search(r'^([A-Za-z\s]+),\s*[A-Z]{2}\s*\([A-Z]{3}\)$', venue_str)
        if pattern1:
            city = pattern1.group(1).strip()
            return {'city': city, 'country_2': country_2}
        
        # PATTERN 2: "Stadium, City, STATE (COUNTRY)" → Extract city from middle
        # Example: "Drake Stadium, Des Moines, IA (USA)" → "Des Moines"
        pattern2 = re.search(r'^[^,]+,\s*([A-Za-z\s]+),\s*[A-Z]{2}\s*\([A-Z]{3}\)$', venue_str)
        if pattern2:
            city = pattern2.group(1).strip()
            return {'city': city, 'country_2': country_2}
        
        # PATTERN 3: "Stadium, City (COUNTRY)" → Extract city after first comma
        # Example: "Olympiastadion, Berlin (GER)" → "Berlin"
        pattern3 = re.search(r'^[^,]+,\s*([^,()]+?)\s*\([A-Z]{3}\)$', venue_str)
        if pattern3:
            city = pattern3.group(1).strip()
            return {'city': city, 'country_2': country_2}
        
        # PATTERN 4: "City (COUNTRY)" → Direct extraction
        # Example: "Paris (FRA)" → "Paris"
        pattern4 = re.search(r'^([^,()]+?)\s*\([A-Z]{3}\)$', venue_str)
        if pattern4:
            city = pattern4.group(1).strip()
            return {'city': city, 'country_2': country_2}
        
        # PATTERN 5: Special cases
        special_cases = {
            'Paris-St-Denis': 'Paris',
            "Villeneuve d'Ascq": 'Lille',
            'Adler, Sochi': 'Sochi',
            'DS, Daegu': 'Daegu',
            'La Cartuja, Sevilla': 'Sevilla'
        }
        
        for special, city in special_cases.items():
            if special in venue_str:
                return {'city': city, 'country_2': country_2}
        
        return {'city': 'Unknown', 'country_2': country_2}
    
    # Standardize city names for better matching with weather data
    city_standardization = {
        'ROMA': 'ROME', 'ATHINA': 'ATHENS', 'BRUXELLES': 'BRUSSELS',
        'LA HABANA': 'HAVANA', 'ZÜRICH': 'ZURICH', 'MÜNCHEN': 'MUNICH',
        'MOSKVA': 'MOSCOW', 'BUCUREŞTI': 'BUCHAREST', 'PRAHA': 'PRAGUE',
        'WARSZAWA': 'WARSAW', 'GÖTEBORG': 'GOTHENBURG', 'KÖLN': 'COLOGNE',
    }
    
    # Get all venues from athletics data
    venues_query = """
    SELECT DISTINCT venue_name
    FROM staging.clean_world_athletics
    WHERE venue_name IS NOT NULL
    """
    
    with engine.connect() as conn:
        venues_df = pd.read_sql(text(venues_query), conn)
    
    # Extract city and country from each venue
    location_info = venues_df['venue_name'].apply(extract_location_from_venue)
    venues_df['city_extracted'] = [info['city'] for info in location_info]
    venues_df['country_extracted'] = [info['country_2'] for info in location_info]
    
    # Standardize city names
    venues_df['city_standardized'] = venues_df['city_extracted'].str.upper().map(city_standardization).fillna(venues_df['city_extracted'].str.upper())
    
    # Get geographic data from cities database
    cities_query = """
    SELECT city_name, country_name, latitude, longitude, altitude, altitude_category
    FROM staging.clean_cities
    WHERE altitude IS NOT NULL
    """
    
    with engine.connect() as conn:
        cities_df = pd.read_sql(text(cities_query), conn)
    
    # Prepare for geographic matching
    venues_df['city_clean'] = venues_df['city_standardized'].str.strip().str.upper()
    venues_df['country_clean'] = venues_df['country_extracted'].str.strip().str.upper()
    cities_df['city_clean'] = cities_df['city_name'].str.strip().str.upper()
    cities_df['country_clean'] = cities_df['country_name'].str.strip().str.upper()
    
    # Try to match with geographic database
    # Strategy 1: City + Country match
    merged_df = venues_df.merge(cities_df, on=['city_clean', 'country_clean'], how='left')
    
    # Strategy 2: City-only match for unmatched venues
    unmatched = merged_df['latitude'].isna()
    if unmatched.sum() > 0:
        logger.info(f"Applying city-only matching for {unmatched.sum()} venues...")
        city_fallback = merged_df[unmatched].drop(columns=['city_name', 'country_name', 'latitude', 'longitude', 'altitude', 'altitude_category']).merge(
            cities_df[['city_clean', 'city_name', 'country_name', 'latitude', 'longitude', 'altitude', 'altitude_category']].drop_duplicates(subset=['city_clean'], keep='first'),
            on='city_clean', how='left'
        )
        
        for col in ['city_name', 'country_name', 'latitude', 'longitude', 'altitude', 'altitude_category']:
            merged_df.loc[unmatched, col] = city_fallback[col].values
    
    # Clean up final data
    merged_df['venue_name_clean'] = merged_df['venue_name'].str.strip().str.title()
    merged_df['city_name'] = merged_df['city_name'].fillna(merged_df['city_standardized']).str.title()
    merged_df['country_name'] = merged_df['country_name'].fillna(merged_df['country_extracted']).str.upper()
    merged_df['country_code'] = merged_df['country_extracted']
    
    # Remove duplicates
    merged_df = merged_df.drop_duplicates(subset='venue_name', keep='first')
    
    # Add climate zone and data quality
    def determine_climate(lat):
        if pd.isna(lat): return 'Unknown'
        abs_lat = abs(lat)
        if abs_lat < 23.5: return 'Tropical'
        elif abs_lat < 40: return 'Subtropical'
        elif abs_lat < 60: return 'Temperate'
        else: return 'Polar'
    
    merged_df['climate_zone'] = merged_df['latitude'].apply(determine_climate)
    merged_df['geographic_source'] = 'Comprehensive_Venue_Analysis'
    merged_df['data_quality_score'] = merged_df.apply(
        lambda row: 9 if (pd.notna(row['latitude']) and pd.notna(row['altitude'])) 
                   else 7 if pd.notna(row['latitude']) else 5, axis=1
    )
    
    # Statistics
    total_venues = len(merged_df)
    city_extraction_success = len(merged_df[merged_df['city_extracted'] != 'Unknown'])
    geographic_match_success = len(merged_df[pd.notna(merged_df['latitude'])])
    
    logger.info(f"City extraction: {city_extraction_success}/{total_venues} ({city_extraction_success/total_venues*100:.1f}%)")
    logger.info(f"Geographic matching: {geographic_match_success}/{total_venues} ({geographic_match_success/total_venues*100:.1f}%)")
    
    # Select final columns
    final_venues = merged_df[[
        'venue_name', 'venue_name_clean', 'city_name', 'country_name', 'country_code',
        'latitude', 'longitude', 'altitude', 'altitude_category', 
        'climate_zone', 'data_quality_score', 'geographic_source'
    ]]

    # Deletion of venues without altitude (doens't end up well in the fact table)
    #final_venues = final_venues.dropna(subset=['altitude'])
    
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
      AND a.event_clean NOT ILIKE '%decathlon%'
    AND a.event_clean NOT ILIKE '%heptathlon%'
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
        athletes = pd.read_sql(text("SELECT athlete_key, athlete_name_clean FROM reconciled.athletes"), conn)
        events = pd.read_sql(text("SELECT event_key, event_name_standardized FROM reconciled.events"), conn)
        venues = pd.read_sql(text("SELECT venue_key, venue_name_clean, city_name FROM reconciled.venues"), conn)
        weather = pd.read_sql(text("SELECT weather_key, venue_name as city_name, month_name FROM reconciled.weather_conditions"), conn)

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


    # OPTIMIZED WEATHER MATCHING - Vectorized approach
    logger.info("Creating weather matching lookup tables...")
    
    # Step 1: Create standardized city names for both datasets
    def standardize_city_name(city):
        if pd.isna(city):
            return 'UNKNOWN'
        return str(city).strip().upper().replace(' ', '').replace('-', '').replace('.', '')
    
    # Apply standardization
    df['city_standardized'] = df['city_name'].apply(standardize_city_name)
    weather['city_standardized'] = weather['city_name'].apply(standardize_city_name)
    
    # Step 2: Create lookup table with exact matches first
    logger.info("Building exact match lookup table...")
    exact_lookup = weather.set_index(['city_standardized', 'month_name'])['weather_key'].to_dict()
    
    # Step 3: Apply exact matches (very fast)
    def get_exact_weather_match(row):
        key = (row['city_standardized'], row['month_name'])
        return exact_lookup.get(key, None)
    
    df['weather_key_exact'] = df.apply(get_exact_weather_match, axis=1)
    exact_matches = df['weather_key_exact'].notna().sum()
    logger.info(f"Exact matches: {exact_matches}/{len(df)} ({exact_matches/len(df)*100:.1f}%)")
    
    # Step 4: For unmatched records, create similarity lookup table
    unmatched_df = df[df['weather_key_exact'].isna()].copy()
    logger.info(f"Creating similarity matches for {len(unmatched_df)} unmatched records...")
    
    if len(unmatched_df) > 0:
        # Create a smaller lookup for just the unique unmatched city/month combinations
        unique_unmatched = unmatched_df[['city_standardized', 'month_name']].drop_duplicates()
        logger.info(f"Only {len(unique_unmatched)} unique city/month combinations need similarity matching")
        
        # Build similarity lookup table only for unique combinations
        similarity_lookup = {}
        weather_cities = weather['city_standardized'].unique()
        
        for _, row in unique_unmatched.iterrows():
            city = row['city_standardized']
            month = row['month_name']
            
            # Find best similarity match
            best_match = None
            best_score = 0
            
            # Get all weather cities for this month
            month_weather = weather[weather['month_name'] == month]
            
            if not month_weather.empty:
                for weather_city in month_weather['city_standardized'].unique():
                    # Calculate similarity score
                    if city == weather_city:
                        score = 100
                    elif city in weather_city or weather_city in city:
                        score = 90
                    else:
                        # Simple Jaccard similarity
                        city_chars = set(city)
                        weather_chars = set(weather_city)
                        if city_chars and weather_chars:
                            intersection = city_chars.intersection(weather_chars)
                            union = city_chars.union(weather_chars)
                            score = len(intersection) / len(union) * 80
                        else:
                            score = 0
                    
                    if score > best_score and score >= 60:  # 60% threshold
                        best_score = score
                        # Get the weather_key for this city/month combination
                        weather_key = month_weather[month_weather['city_standardized'] == weather_city]['weather_key'].iloc[0]
                        best_match = weather_key
            
            similarity_lookup[(city, month)] = best_match
        
        # Apply similarity matches
        def get_similarity_weather_match(row):
            if pd.notna(row['weather_key_exact']):
                return row['weather_key_exact']
            key = (row['city_standardized'], row['month_name'])
            return similarity_lookup.get(key, 1)  # Default to weather_key=1
        
        df['weather_key'] = df.apply(get_similarity_weather_match, axis=1)
        
        similarity_matches = (df['weather_key'] != 1).sum() - exact_matches
        logger.info(f"Similarity matches: {similarity_matches} additional matches")
    else:
        df['weather_key'] = df['weather_key_exact'].fillna(1)
    
    total_matches = (df['weather_key'] != 1).sum()
    logger.info(f"Total weather matching success: {total_matches}/{len(df)} ({total_matches/len(df)*100:.1f}%)")
    logger.info(f"STEP 5 - After weather join: {len(df)} records")
    

    # Remove performances without weather data at reconciled layer
    logger.info("Removing performances without weather data...")
    
    initial_count = len(df)
    
    # Keep only performances with actual weather matches (not default)
    df_with_weather = df.dropna(subset=['weather_key'])
    
    removed_count = initial_count - len(df_with_weather)
    removal_percentage = (removed_count / initial_count) * 100
    
    logger.info(f"Reconciled layer weather filter:")
    logger.info(f"  Initial performances: {initial_count:,}")
    logger.info(f"  Removed (no weather): {removed_count:,} ({removal_percentage:.1f}%)")
    logger.info(f"  Reconciled with weather: {len(df_with_weather):,}")

    # Update dataframe
    df = df_with_weather


    # Data quality filtering
    logger.info(f"Before filtering: {len(df)} records")
    df = df.dropna(subset=['athlete_key', 'event_key']).copy()
    logger.info(f"After requiring athlete/event IDs: {len(df)} records")
    
    # Fill missing foreign keys with default values
    df['venue_key'] = df['venue_key'].fillna(1)  # Default venue
    df['weather_key'] = df['weather_key'].fillna(1)  # Default weather
    df['data_quality_score'] = 8
    # Add created_date column to match SQL table structure
    df['created_date'] = pd.Timestamp.now()

    # Select final columns
    final = df[['athlete_key', 'event_key', 'venue_key', 'weather_key',
                'competition_date', 'result_value', 'wind_reading', 'position_finish',
                'data_source', 'data_quality_score', 'created_date']]

    initial_count = len(df)
    final_new = final.drop_duplicates(subset=[
    'athlete_key', 'event_key', 'venue_key', 'weather_key', 'competition_date'])
    new_count = len(final_new)
    logger.info(f"Filter duplicated performances: {initial_count - new_count} performances removed")

    final = final_new
            

    # Convert data types
    final['athlete_key'] = final['athlete_key'].astype(int)
    final['event_key'] = final['event_key'].astype(int) 
    final['venue_key'] = final['venue_key'].astype(int)
    final['weather_key'] = final['weather_key'].astype(int)

    logger.info(f"Final performance records ready for insert: {len(final)}")
    logger.info(f"Weather match success: {(final['weather_key'] != 1).sum()}/{len(final)} performances have weather data")

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
    logger.info(f"Appending {len(df)} records to {schema}.{table_name}...")
    
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
            
            logger.info(f"{len(df)} records appended successfully")
            
        except Exception as e:
            logger.error(f"COPY failed: {e}")
            logger.error(f"Attempted COPY command: {copy_sql}")
            raise



def clear_reconciled_tables(engine):
    """Clear existing data before re-loading"""
    tables = ['performances', 'weather_conditions', 'venues', 'events', 'athletes']
    
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
        reconcile_performances(engine)

        # Count queries
        with engine.connect() as conn:
            tables = ['athletes', 'events', 'venues', 'weather_conditions', 'performances']
            for table in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM reconciled.{table}")).scalar()
                logger.info(f"reconciled.{table}: {count} records")

        logger.info("Reconciled data layer created successfully.")
    except Exception as e:
        logger.error(f"Reconciled data layer creation failed: {e}")
        raise

if __name__ == "__main__":
    main()
