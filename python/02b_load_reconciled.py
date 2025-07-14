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


    # Remove multi-events (Decathlon, Heptathlon)
    logger.info("Filtering out multi-events...")
    initial_count = len(df)

    df = df[~df['event_name'].str.contains('(?i)(decathlon|heptathlon)', na=False)]

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
        logger.info(f"Adding weather estimates for {len(missing_cities)} missing athletics cities: {missing_cities}")
        
        # Simple climate-based estimates for missing cities
        city_climate_estimates = {
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
            logger.info(f"Added {len(estimates)} weather estimate records for {len(set(est['venue_name'] for est in estimates))} cities")

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
    Reconcile venues with improved city extraction and PRESERVED venue names
    """
    logger.info("Reconciling venues with preserved original names...")
    
    # Comprehensive venue-to-city mapping
    VENUE_CITY_MAPPING = {
        # German venues
        'OLYMPIASTADION, BERLIN': 'BERLIN',
        'BERLIN': 'BERLIN',
        'OLYMPIASTADION, MUNICH': 'MUNICH',
        'MÜNCHEN': 'MUNICH',
        'MUNICH': 'MUNICH',
        
        # UK venues
        'OLYMPIC STADIUM, LONDON': 'LONDON',
        'LONDON': 'LONDON',
        'CRYSTAL PALACE': 'LONDON',
        'BIRMINGHAM': 'BIRMINGHAM',
        'MANCHESTER': 'MANCHESTER',
        
        # USA venues
        'HAYWARD FIELD': 'EUGENE',
        'EUGENE': 'EUGENE',
        'SACRAMENTO': 'SACRAMENTO',
        'STANFORD': 'PALO ALTO',
        'MT. SAC': 'LOS ANGELES',
        'WALNUT': 'LOS ANGELES',
        'DRAKE STADIUM': 'DES MOINES',
        'NEW YORK': 'NEW YORK',
        'BOSTON': 'BOSTON',
        'INDIANAPOLIS': 'INDIANAPOLIS',
        'AUSTIN': 'AUSTIN',
        'GAINESVILLE': 'GAINESVILLE',
        'KNOXVILLE': 'KNOXVILLE',
        
        # Other major venues
        'STADE DE FRANCE': 'PARIS',
        'PARIS': 'PARIS',
        'STADIO OLIMPICO': 'ROME',
        'ROME': 'ROME',
        'BISLETT': 'OSLO',
        'OSLO': 'OSLO',
        'ZURICH': 'ZURICH',
        'BRUSSELS': 'BRUSSELS',
        'MONACO': 'MONACO',
        'LAUSANNE': 'LAUSANNE',
        'KINGSTON': 'KINGSTON',
        'DOHA': 'DOHA',
        'SHANGHAI': 'SHANGHAI',
        'BEIJING': 'BEIJING',
        'ATHENS': 'ATHENS',
        'OLYMPIC STADIUM, ATHENS': 'ATHENS',
        'SYDNEY': 'SYDNEY',
        'MELBOURNE': 'MELBOURNE',
        'TOKYO': 'TOKYO',
        'DUBAI': 'DUBAI',
        'NAIROBI': 'NAIROBI'
    }
    
    def extract_city_from_venue_improved(venue_name):
        """Extract city with better pattern matching"""
        if pd.isna(venue_name):
            return {'city': 'Unknown', 'country_code': 'XX', 'confidence': 0}
        
        venue_str = str(venue_name).strip()
        venue_upper = venue_str.upper()
        
        # Extract country code
        import re
        country_match = re.search(r'\(([A-Z]{3})\)', venue_str)
        country_3 = country_match.group(1) if country_match else 'Unknown'
        
        # Country mapping
        country_mapping = {
            'USA': 'US', 'GBR': 'GB', 'GER': 'DE', 'FRA': 'FR', 'ITA': 'IT',
            'SUI': 'CH', 'BEL': 'BE', 'SWE': 'SE', 'FIN': 'FI', 'GRE': 'GR',
            'CHN': 'CN', 'JAM': 'JM', 'CUB': 'CU', 'MON': 'MC', 'RUS': 'RU',
            'NED': 'NL', 'ESP': 'ES', 'JPN': 'JP', 'HUN': 'HU', 'AUT': 'AT',
            'POL': 'PL', 'CZE': 'CZ', 'BRA': 'BR', 'QAT': 'QA', 'UKR': 'UA',
            'AUS': 'AU', 'CRO': 'HR', 'ROU': 'RO', 'BUL': 'BG', 'KOR': 'KR',
            'KEN': 'KE', 'ETH': 'ET', 'RSA': 'ZA', 'NZL': 'NZ', 'CAN': 'CA',
            'DEN': 'DK', 'NOR': 'NO', 'POR': 'PT', 'IRL': 'IE', 'MEX': 'MX'
        }
        country_2 = country_mapping.get(country_3, 'XX')
        
        # Check known venues first
        for venue_pattern, city in VENUE_CITY_MAPPING.items():
            if venue_pattern in venue_upper:
                return {'city': city, 'country_code': country_2, 'confidence': 95}
        
        # Parse venue structure
        venue_no_country = re.sub(r'\s*\([A-Z]{3}\)\s*$', '', venue_str).strip()
        parts = venue_no_country.split(',')
        
        if len(parts) >= 2:
            # "Stadium, City" or "Stadium, City, State"
            city_part = parts[1].strip()
            # Remove state codes
            city_part = re.sub(r'\s+[A-Z]{2}$', '', city_part).strip()
            if city_part and len(city_part) > 2:
                return {'city': city_part.upper(), 'country_code': country_2, 'confidence': 85}
        elif len(parts) == 1:
            # Direct city name
            if not any(word in venue_upper for word in ['STADIUM', 'FIELD', 'TRACK', 'ARENA', 'CENTRE']):
                return {'city': venue_no_country.upper(), 'country_code': country_2, 'confidence': 75}
        
        return {'city': 'Unknown', 'country_code': country_2, 'confidence': 20}
    
    # Get all venues
    with engine.connect() as conn:
        venues_df = pd.read_sql(text("""
            SELECT DISTINCT venue_name
            FROM staging.clean_world_athletics
            WHERE venue_name IS NOT NULL
            ORDER BY venue_name
        """), conn)
    
    logger.info(f"Processing {len(venues_df)} unique venues...")
    
    # Extract cities
    extraction_results = venues_df['venue_name'].apply(extract_city_from_venue_improved)
    venues_df['city_extracted'] = [r['city'] for r in extraction_results]
    venues_df['country_code'] = [r['country_code'] for r in extraction_results]
    venues_df['extraction_confidence'] = [r['confidence'] for r in extraction_results]
    
    # IMPORTANT: Preserve original venue name, only clean for display
    venues_df['venue_name_clean'] = venues_df['venue_name'].str.strip().str.title()
    
    # Get geographic data
    with engine.connect() as conn:
        cities_df = pd.read_sql(text("""
            SELECT city_name, country_name, latitude, longitude, altitude, altitude_category
            FROM staging.clean_cities
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """), conn)
    
    # Prepare for matching
    venues_df['city_for_match'] = venues_df['city_extracted'].str.strip().str.upper()
    venues_df['country_for_match'] = venues_df['country_code'].str.strip().str.upper()
    cities_df['city_for_match'] = cities_df['city_name'].str.strip().str.upper()
    cities_df['country_for_match'] = cities_df['country_name'].str.strip().str.upper()
    
    # Match with geographic data
    merged_df = venues_df.merge(
        cities_df,
        left_on=['city_for_match', 'country_for_match'],
        right_on=['city_for_match', 'country_for_match'],
        how='left'
    )
    
    # For high-confidence unmatched, try conservative city-only match
    unmatched = merged_df['latitude'].isna() & (merged_df['extraction_confidence'] >= 85)
    if unmatched.sum() > 0:
        logger.info(f"Attempting city-only match for {unmatched.sum()} high-confidence unmatched venues...")
        
        # Only match known athletics cities
        known_athletics_cities = set(VENUE_CITY_MAPPING.values())
        
        for idx in merged_df[unmatched].index:
            city = merged_df.loc[idx, 'city_for_match']
            if city in known_athletics_cities:
                city_match = cities_df[cities_df['city_for_match'] == city].head(1)
                if not city_match.empty:
                    for col in ['city_name', 'country_name', 'latitude', 'longitude', 'altitude', 'altitude_category']:
                        merged_df.loc[idx, col] = city_match.iloc[0][col]
    
    # Clean up
    merged_df['city_name'] = merged_df['city_name'].fillna(merged_df['city_extracted']).str.title()
    merged_df['country_name'] = merged_df['country_name'].fillna('Unknown').str.upper()
    
    # Add derived fields
    def determine_climate(lat):
        if pd.isna(lat): return 'Unknown'
        abs_lat = abs(lat)
        if abs_lat < 23.5: return 'Tropical'
        elif abs_lat < 40: return 'Subtropical'
        elif abs_lat < 60: return 'Temperate'
        else: return 'Polar'
    
    merged_df['climate_zone'] = merged_df['latitude'].apply(determine_climate)
    merged_df['geographic_source'] = 'Precise_Venue_Matching'
    merged_df['data_quality_score'] = merged_df.apply(
        lambda row: 9 if (pd.notna(row['latitude']) and row['extraction_confidence'] >= 85) 
                   else 7 if pd.notna(row['latitude'])
                   else 5 if row['extraction_confidence'] >= 50
                   else 3, axis=1
    )
    
    # Select final columns - KEEP ORIGINAL venue_name
    final_venues = merged_df[[
        'venue_name',  # Original, not cleaned!
        'venue_name_clean', 
        'city_name', 
        'country_name', 
        'country_code',
        'latitude', 
        'longitude', 
        'altitude', 
        'altitude_category', 
        'climate_zone', 
        'data_quality_score', 
        'geographic_source'
    ]].drop_duplicates(subset='venue_name', keep='first')
    
    # Log some key venues for verification
    logger.info("\nKey venue mappings:")
    key_venues = ['Olympiastadion, Berlin (GER)', 'Olympic Stadium, London (GBR)', 
                  'Stade de France, Paris (FRA)', 'Hayward Field, Eugene, OR (USA)']
    for venue in key_venues:
        match = final_venues[final_venues['venue_name'] == venue]
        if not match.empty:
            logger.info(f"  {venue} → {match.iloc[0]['city_name']}, {match.iloc[0]['country_name']}")
    
    matched_count = (final_venues['latitude'].notna()).sum()
    logger.info(f"Geographic matching: {matched_count}/{len(final_venues)} ({matched_count/len(final_venues)*100:.1f}%)")
    
    # Save to database
    with engine.connect() as conn:
        final_venues.to_sql('venues', conn, schema='reconciled', if_exists='append', index=False)
        conn.commit()
    
    logger.info(f"Inserted {len(final_venues)} venues with preserved names")
    return final_venues



def reconcile_performances(engine):
    logger.info("Reconciling performances with improved venue matching...")

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

    logger.info(f"Original performance data: {len(df)} records")

    # Extract month from performance date for weather matching
    df['competition_date_parsed'] = pd.to_datetime(df['competition_date'], errors='coerce')
    df['month'] = df['competition_date_parsed'].dt.month
    df['month_name'] = df['month'].apply(lambda x: calendar.month_name[int(x)] if not pd.isna(x) else 'Unknown')
    
    # Clean names for matching - BUT KEEP ORIGINAL VENUE NAME
    df['athlete_name_clean'] = df['athlete_name'].str.strip().str.title()
    df['event_name_clean'] = df['event_name']
    # DON'T clean venue name for matching - use exact match
    df['venue_name_for_match'] = df['venue_name'].str.strip()  # Only strip whitespace

    # Read reference tables - FIX: Include country_name
    with engine.connect() as conn:
        athletes = pd.read_sql(text("SELECT athlete_key, athlete_name_clean FROM reconciled.athletes"), conn)
        events = pd.read_sql(text("SELECT event_key, event_name_standardized FROM reconciled.events"), conn)
        # Get venues with ORIGINAL venue_name for exact matching
        venues = pd.read_sql(text("""
            SELECT venue_key, venue_name, venue_name_clean, city_name, country_name 
            FROM reconciled.venues
        """), conn)
        weather = pd.read_sql(text("SELECT weather_key, venue_name as city_name, month_name FROM reconciled.weather_conditions"), conn)

    logger.info(f"Reference tables - Athletes: {len(athletes)}, Events: {len(events)}, Venues: {len(venues)}, Weather: {len(weather)}")

    # Join dimensions
    df = df.merge(athletes, on='athlete_name_clean', how='left')
    df = df.merge(events, left_on='event_name_clean', right_on='event_name_standardized', how='left')
    
    # FIXED: Match venues on EXACT venue_name, not cleaned version
    venues['venue_name_for_match'] = venues['venue_name'].str.strip()
    df = df.merge(venues[['venue_key', 'venue_name_for_match', 'city_name', 'country_name']], 
                  on='venue_name_for_match', how='left')
    
    # Log venue matching success
    venue_matched = df['venue_key'].notna().sum()
    logger.info(f"Venue matching: {venue_matched}/{len(df)} ({venue_matched/len(df)*100:.1f}%)")
    
    # Sample check for known performances
    bolt_check = df[df['athlete_name'].str.contains('Bolt', case=False, na=False) & 
                    (df['result_value'] < 10)]
    if not bolt_check.empty:
        logger.info("\nSample venue matches for Bolt's 100m performances:")
        for _, row in bolt_check.head(5).iterrows():
            logger.info(f"  {row['result_value']:.2f}s at '{row['venue_name']}' → city: {row['city_name']}")

    logger.info(f"After dimension joins: {len(df)} records")

    # IMPROVED WEATHER MATCHING
    logger.info("Implementing weather matching...")
    
    # Create validated city list from weather data
    weather_cities = set(weather['city_name'].unique())
    
    # Strategy 1: Direct exact match (city + month)
    df['city_upper'] = df['city_name'].str.upper() if 'city_name' in df.columns else None
    weather['city_upper'] = weather['city_name'].str.upper()
    
    # Create weather lookup
    weather_lookup = weather.set_index(['city_upper', 'month_name'])['weather_key'].to_dict()
    
    def get_weather_match(row):
        """Conservative weather matching"""
        if pd.isna(row.get('city_name')) or row.get('city_name') == 'Unknown':
            return None
            
        city = str(row.get('city_upper', '')).upper()
        month = row['month_name']
        
        # Direct lookup
        direct_key = (city, month)
        if direct_key in weather_lookup:
            return weather_lookup[direct_key]
        
        # Try common city name variations
        city_variations = {
            'NEW YORK': ['NEW YORK CITY', 'NYC'],
            'LOS ANGELES': ['LA'],
            'SAN FRANCISCO': ['SF', 'PALO ALTO'],
            'WASHINGTON': ['WASHINGTON DC', 'DC']
        }
        
        for main_city, variations in city_variations.items():
            if city in variations:
                alt_key = (main_city, month)
                if alt_key in weather_lookup:
                    return weather_lookup[alt_key]
        
        return None
    
    # Apply weather matching
    df['weather_key'] = df.apply(get_weather_match, axis=1)
    
    # For missing weather, use default
    weather_matched = df['weather_key'].notna().sum()
    logger.info(f"Weather matching: {weather_matched}/{len(df)} ({weather_matched/len(df)*100:.1f}%)")
    
    #df['weather_key'] = df['weather_key'].fillna(1)  # Default weather key
    
    # Remove performances without critical dimensions
    initial_count = len(df)
    df = df.dropna(subset=['athlete_key', 'event_key'])
    logger.info(f"After requiring athlete/event: {len(df)} records (removed {initial_count - len(df)})")

    initial_count = len(df)
    df = df.dropna(subset=['weather_key'])
    logger.info(f"After requiring weather data: {len(df)} records (removed {initial_count - len(df)})")
    
    # Fill missing foreign keys
    df['venue_key'] = df['venue_key'].fillna(1)
    df['data_quality_score'] = 8
    df['created_date'] = pd.Timestamp.now()

    # Select final columns
    final = df[['athlete_key', 'event_key', 'venue_key', 'weather_key',
                'competition_date', 'result_value', 'wind_reading', 'position_finish',
                'data_source', 'data_quality_score', 'created_date']]

    # Convert data types
    final['athlete_key'] = final['athlete_key'].astype(int)
    final['event_key'] = final['event_key'].astype(int) 
    final['venue_key'] = final['venue_key'].astype(int)
    final['weather_key'] = final['weather_key'].astype(int)

    logger.info(f"Final performance records: {len(final)}")
    
    # Clear and save
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE reconciled.performances RESTART IDENTITY"))
        conn.commit()
    
    # Use the ultra-fast append
    ultra_fast_postgres_append(final, 'performances', engine)
    
    logger.info(f"Inserted {len(final)} performances with correct venue matching")
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
