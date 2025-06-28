"""Data Transformation Script - Clean and standardize data"""

import io
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import CONNECTION_STRING, EVENT_CATEGORIES, COMPETITION_LEVELS, DATA_QUALITY
import logging
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_db_connection():
    """Create database connection"""
    engine = create_engine(CONNECTION_STRING)
    return engine



# def clean_world_athletics_data(engine):
#     """Clean and standardize World Athletics data"""
#     try:
#         logger.info("Cleaning World Athletics data...")
        
#         with engine.connect() as conn:
#             df = pd.read_sql(text("SELECT * FROM staging.raw_world_athletics"), conn)
        
#         logger.info(f"Original records: {len(df)}")
#         logger.info(f"Columns: {list(df.columns)}")
        
#         # Standardize column names
#         column_mapping = {
#             'Competitor': 'athlete_name',  
#             'Event': 'event_name',
#             # 'Mark': 'result_value',       
#             'Venue': 'venue_name',
#             'Date': 'competition_date',
#             'Nat': 'nationality',         
#             'Sex': 'gender',               
#             'DOB': 'date_of_birth',        
#             'Rank': 'rank_position',      
#             'Wind': 'wind_reading'         
#         }
        
#         # Rename columns if they exist
#         for old_col, new_col in column_mapping.items():
#             if old_col in df.columns:
#                 df = df.rename(columns={old_col: new_col})
        
#         # Data cleaning
#         # Remove null essential fields
#         initial_count = len(df)
#         df = df.dropna(subset=['athlete_name', 'event_name', 'result_value'])
#         logger.info(f"After removing null essential fields: {len(df)} records (-{initial_count - len(df)})")
        
#         # Clean result values
#         df = clean_result_values(df)
        
#         # Standardize event names
#         df = standardize_event_names(df)
        
#         # Clean athlete names
#         logger.info("Cleaning athlete names...")
#         df['athlete_name'] = df['athlete_name'].str.strip().str.upper()  

#         # Clean venue names:
#         if 'venue_name' in df.columns:
#             logger.info("Cleaning venue names...")
#             df['venue_name'] = df['venue_name'].str.strip().str.upper()
            
#         # Add competition level (if not present)
#         if 'competition_level' not in df.columns:
#             df['competition_level'] = 'Professional'  # Default value
        
#         # Add data source
#         df['data_source'] = 'WorldAthletics'


#         # Save cleaned data
#         #with engine.connect() as conn:
#             #df.to_sql('clean_world_athletics', conn, schema='staging', 
#                      #if_exists='replace', index=False, method='multi')
#         chunked_save_to_postgres(df, 'clean_world_athletics', engine) 
                
#         logger.info(f"Cleaned World Athletics data: {len(df)} records saved")
#         return df
        
#     except Exception as e:
#         logger.error(f"Failed to clean World Athletics data: {e}")
#         raise


def clean_world_athletics_data(engine):
    """Clean and standardize World Athletics data"""
    try:
        logger.info("Cleaning World Athletics data...")
        
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM staging.raw_world_athletics"), conn)
        
        logger.info(f"Original records: {len(df)}")
        logger.info(f"Original columns: {list(df.columns)}")
        
        # Standardize column names - RENAME AND SELECT ONLY NEEDED COLUMNS
        df_clean = pd.DataFrame({
            'athlete_name': df['Competitor'],
            'event_name': df['Event'],
            'result_value': df['Mark'],
            'venue_name': df['Venue'],
            'competition_date': df['Date'],
            'nationality': df['Nat'],
            'gender': df['Sex'],
            'date_of_birth': df['DOB'],
            'rank_position': df['Rank'],
            'wind_reading': df['Wind'],
            'pos': df['Pos'], 
            'results_score': df['Results Score']  
        })
        
        logger.info(f"After column selection: {list(df_clean.columns)}")
        
        # Data cleaning
        initial_count = len(df_clean)
        df_clean = df_clean.dropna(subset=['athlete_name', 'event_name', 'result_value'])
        logger.info(f"After removing null essential fields: {len(df_clean)} records (-{initial_count - len(df_clean)})")
        
        # Clean result values
        df_clean = clean_result_values(df_clean)
        
        # Standardize event names
        df_clean = standardize_event_names(df_clean)
        
        # Clean athlete and venue names
        logger.info("Cleaning athlete names...")
        df_clean['athlete_name'] = df_clean['athlete_name'].str.strip().str.upper()
        
        # Clean venue names:
        if 'venue_name' in df.columns:
            logger.info("Cleaning venue names...")
            df_clean['venue_name'] = df_clean['venue_name'].str.strip().str.upper()
        
        # Add computed columns
        df_clean['competition_level'] = 'Professional'  # Default value
        
        logger.info(f"Final columns: {list(df_clean.columns)}")
        logger.info(f"Final column count: {len(df_clean.columns)}")
        
        # Save cleaned data
        chunked_save_to_postgres(df_clean, 'clean_world_athletics', engine)
        
        logger.info(f"Cleaned World Athletics data: {len(df_clean)} records saved")
        return df_clean
    
    except Exception as e:
        logger.error(f"Failed to clean World Athletics data: {e}")
        raise



def clean_result_values(df):
    """Clean and convert result values to numeric"""
    logger.info("Cleaning result values...")
    
    def parse_time_result(result_str):
        """Convert time strings to seconds"""
        try:
            result_str = str(result_str).strip()
            
            # Handle DNF, DQ, etc.
            if result_str in ['DNF', 'DQ', 'DNS', 'NM', '']:
                return None
                
            # Handle time formats (MM:SS.ss or HH:MM:SS.ss)
            if ':' in result_str:
                parts = result_str.split(':')
                if len(parts) == 2:  # MM:SS.ss
                    minutes = float(parts[0])
                    seconds = float(parts[1])
                    return minutes * 60 + seconds
                elif len(parts) == 3:  # HH:MM:SS.ss
                    hours = float(parts[0])
                    minutes = float(parts[1])
                    seconds = float(parts[2])
                    return hours * 3600 + minutes * 60 + seconds
            else:
                # Direct numeric value (seconds for time, meters for distance)
                return float(result_str)
                
        except (ValueError, AttributeError):
            return None
    
    # Apply conversion
    df['result_numeric'] = df['result_value'].apply(parse_time_result)
    
    # Filter out invalid results
    initial_count = len(df)
    df = df.dropna(subset=['result_numeric'])
    df = df[(df['result_numeric'] >= DATA_QUALITY['min_result_value']) & 
            (df['result_numeric'] <= DATA_QUALITY['max_result_value'])]
    
    logger.info(f"After cleaning result values: {len(df)} records (-{initial_count - len(df)} invalid)")
    
    return df



def standardize_event_names(df):
    """Standardize event names across datasets"""
    logger.info("Standardizing event names...")
    
    event_mapping = {
        # Sprint events
        '100m': '100m', '100 metres': '100m', '100 meters': '100m', '100M': '100m',
        '200m': '200m', '200 metres': '200m', '200 meters': '200m', '200M': '200m',
        '400m': '400m', '400 metres': '400m', '400 meters': '400m', '400M': '400m',
        '60m': '60m', '60 metres': '60m', '60 meters': '60m',
        
        # Distance events
        '800m': '800m', '800 metres': '800m', '800 meters': '800m',
        '1500m': '1500m', '1500 metres': '1500m', '1500 meters': '1500m',
        '5000m': '5000m', '5000 metres': '5000m', '5000 meters': '5000m',
        '10000m': '10000m', '10000 metres': '10000m', '10000 meters': '10000m',
        'Marathon': 'Marathon', 'marathon': 'Marathon',
        
        # Hurdles
        '110m Hurdles': '110m Hurdles', '110m hurdles': '110m Hurdles',
        '100m Hurdles': '100m Hurdles', '100m hurdles': '100m Hurdles',
        '400m Hurdles': '400m Hurdles', '400m hurdles': '400m Hurdles',
        
        # Jumps
        'Long Jump': 'Long Jump', 'long jump': 'Long Jump', 'LJ': 'Long Jump',
        'High Jump': 'High Jump', 'high jump': 'High Jump', 'HJ': 'High Jump',
        'Triple Jump': 'Triple Jump', 'triple jump': 'Triple Jump', 'TJ': 'Triple Jump',
        'Pole Vault': 'Pole Vault', 'pole vault': 'Pole Vault', 'PV': 'Pole Vault',
        
        # Throws
        'Shot Put': 'Shot Put', 'shot put': 'Shot Put', 'SP': 'Shot Put',
        'Discus Throw': 'Discus Throw', 'discus throw': 'Discus Throw', 'DT': 'Discus Throw',
        'Hammer Throw': 'Hammer Throw', 'hammer throw': 'Hammer Throw', 'HT': 'Hammer Throw',
        'Javelin Throw': 'Javelin Throw', 'javelin throw': 'Javelin Throw', 'JT': 'Javelin Throw'
    }
    
    df['event_clean'] = df['event_name'].map(event_mapping).fillna(df['event_name'])
    
    # Log event standardization results
    unique_events = df['event_clean'].unique()
    logger.info(f"Standardized events: {sorted(unique_events)}")
    
    return df



# def integrate_geographic_data(engine):
#     """Integrate and clean geographic data"""
#     try:
#         logger.info("Integrating geographic data...")
        
#         with engine.connect() as conn:
#             cities_df = pd.read_sql(text("SELECT * FROM staging.raw_cities"), conn)
        
#         # String cleaning
#         logger.info("Cleaning city and country names (3.2M records)...")
#         cities_df['city_clean'] = cities_df['City'].str.strip().str.upper()   
#         cities_df['country_clean'] = cities_df['Country'].str.strip().str.upper() 
        
#         # Handle missing values
#         cities_df['Population'] = cities_df['Population'].fillna(0)  
        
#         # Altitude categorization
#         def categorize_altitude(lat):
#             if pd.isna(lat):
#                 return 'Unknown'
#             if abs(lat) < 30:
#                 return 'Sea Level'
#             elif abs(lat) < 50:
#                 return 'Moderate'
#             else:
#                 return 'High'
        
#         logger.info("Calculating altitude categories...")
#         cities_df['altitude_category'] = cities_df['Latitude'].apply(categorize_altitude)  
        
#         # Save cleaned geographic data
#         logger.info("Saving geographic data...")
#         #with engine.connect() as conn:
#         #    cities_df.to_sql('clean_cities', conn, schema='staging', 
#         #                    if_exists='replace', index=False, method='multi')
#         chunked_save_to_postgres(cities_df, 'clean_cities', engine)

#         logger.info(f"Geographic data integrated: {len(cities_df)} cities")
#         return cities_df
        
#     except Exception as e:
#         logger.error(f"Failed to integrate geographic data: {e}")
#         raise

def integrate_geographic_data(engine):
    """Integrate and clean geographic data"""
    try:
        logger.info("Integrating geographic data...")
        
        with engine.connect() as conn:
            cities_df = pd.read_sql(text("SELECT * FROM staging.raw_cities"), conn)
        
        logger.info(f"Original cities data: {len(cities_df)} records")
        logger.info(f"Original columns: {list(cities_df.columns)}")
        
        # CREATE CLEAN DATAFRAME WITH ONLY NEEDED COLUMNS
        cities_clean = pd.DataFrame({
            'city_name': cities_df['City'].str.strip().str.upper(),
            'country_name': cities_df['Country'].str.strip().str.upper(),
            'latitude': cities_df['Latitude'],   
            'longitude': cities_df['Longitude']  
        })
        
        # Calculate altitude categories from latitude
        def categorize_altitude(lat):
            if pd.isna(lat):
                return 'Unknown'
            if abs(lat) < 30:
                return 'Sea Level'
            elif abs(lat) < 50:
                return 'Moderate'
            else:
                return 'High'
        
        logger.info("Calculating altitude categories...")
        cities_clean['altitude_category'] = cities_clean['latitude'].apply(categorize_altitude)
        
        # Add data source
        cities_clean['data_source'] = 'WorldCities'
        
        logger.info(f"Clean cities columns: {list(cities_clean.columns)}")
        logger.info(f"Clean cities count: {len(cities_clean)}")
        
        # Save cleaned geographic data
        chunked_save_to_postgres(cities_clean, 'clean_cities', engine)
        
        logger.info(f"Geographic data integrated: {len(cities_clean)} cities")
        return cities_clean
        
    except Exception as e:
        logger.error(f"Failed to integrate geographic data: {e}")
        raise



def integrate_temperature_data(engine):
    """Integrate and clean temperature data"""
    try:
        logger.info("Integrating temperature data...")
        
        with engine.connect() as conn:
            temp_df = pd.read_sql(text("SELECT * FROM staging.raw_temperature"), conn)
        
        # Clean temperature data
        temp_df = temp_df.dropna(subset=['AvgTemperature']) 
        temp_df = temp_df[(temp_df['Year'] >= DATA_QUALITY['min_year']) & 
                         (temp_df['Year'] <= DATA_QUALITY['max_year'])]
        
        logger.info(f"Temperature data after filtering: {len(temp_df)} records")
        
        # Calculate monthly averages by city
        monthly_avg = temp_df.groupby(['City', 'Country', 'Month'])['AvgTemperature'].mean().reset_index()
        
        logger.info(f"Monthly averages calculated: {len(monthly_avg)} records")
        
        # Convert Fahrenheit to Celsius if needed
        # Check if data looks like Fahrenheit (temperatures > 40 are likely Fahrenheit)
        if monthly_avg['AvgTemperature'].mean() > 40:
            logger.info("Converting Fahrenheit to Celsius...")
            monthly_avg['AvgTemperature'] = (monthly_avg['AvgTemperature'] - 32) * 5/9
        
        # Add temperature categories (Celsius thresholds)
        def categorize_temperature(temp):
            if temp < 10:
                return 'Cold'
            elif temp < 18:
                return 'Cool'
            elif temp < 24:
                return 'Moderate'
            elif temp < 30:
                return 'Warm'
            else:
                return 'Hot'
        
        monthly_avg['temperature_category'] = monthly_avg['AvgTemperature'].apply(categorize_temperature)
        
        # Save cleaned temperature data
        #with engine.connect() as conn:
        #    monthly_avg.to_sql('clean_temperature', conn, schema='staging', 
        #                      if_exists='replace', index=False, method='multi')
        logger.info("Fast saving temperature data...")
        chunked_save_to_postgres(monthly_avg, 'clean_temperature', engine)

        logger.info(f"Temperature data integrated: {len(monthly_avg)} records")
        return monthly_avg
        
    except Exception as e:
        logger.error(f"Failed to integrate temperature data: {e}")
        raise



def chunked_save_to_postgres(df, table_name, engine, schema='staging', chunk_size=10000):
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


        
def main():
    """Main transformation process"""
    try:
        logger.info("Starting data transformation process...")
        
        # Create database connection
        engine = create_db_connection()
        
        # Clean and transform each dataset
        athletics_df = clean_world_athletics_data(engine)
        cities_df = integrate_geographic_data(engine)
        temperature_df = integrate_temperature_data(engine)
        
        logger.info("Data transformation completed successfully!")
        
        # Summary statistics
        logger.info(f"Final record counts:")
        logger.info(f"  Athletics: {len(athletics_df)}")
        logger.info(f"  Cities: {len(cities_df)}")
        logger.info(f"  Temperature: {len(temperature_df)}")
        
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()