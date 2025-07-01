"""Data Extraction Script - Load raw data to staging tables"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import CONNECTION_STRING, DATA_PATHS
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_db_connection():
    """Create database connection"""
    try:
        engine = create_engine(CONNECTION_STRING)
        logger.info("Database connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def extract_world_athletics_data():
    """Load World Athletics Database"""
    try:
        logger.info("Loading World Athletics Database...")
        df = pd.read_csv(
            DATA_PATHS['world_athletics'], 
            delimiter=';',
            encoding='utf-8',  # Handle special characters
            quotechar='"',     # Handle quoted fields
            on_bad_lines='skip'  # Skip problematic lines
        )
        logger.info(f"World Athletics Database loaded: {len(df)} records")
        
        # Clean column names (remove extra spaces)
        df.columns = df.columns.str.strip()
        
        # Display basic info
        logger.info(f"Columns: {list(df.columns)}")
        if 'Date' in df.columns:
            logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        
        return df
    except Exception as e:
        logger.error(f"Failed to load World Athletics data: {e}")
        raise


# def extract_cities_data():
#     """Load World Cities Database"""
#     try:
#         logger.info("Loading World Cities Database...")
#         df = pd.read_csv(DATA_PATHS['cities'])
#         logger.info(f"Cities Database loaded: {len(df)} records")
        
#         # Display basic info
#         logger.info(f"Countries: {df['country'].nunique() if 'country' in df.columns else 'No country column'}")
        
#         return df
#     except Exception as e:
#         logger.error(f"Failed to load Cities data: {e}")
#         raise

def extract_cities_data():
    """Load GeoNames Cities Database with Elevation"""
    try:
        logger.info("Loading GeoNames Cities Database...")
        
        # Define the column names for GeoNames format
        geonames_columns = [
            'geonameid', 'name', 'asciiname', 'alternatenames', 
            'latitude', 'longitude', 'feature_class', 'feature_code',
            'country_code', 'cc2', 'admin1_code', 'admin2_code', 
            'admin3_code', 'admin4_code', 'population', 'dem', 'elevation', 
            'continent', 'modification_date'
        ]
        
        # Read the CSV with proper column names
        df = pd.read_csv(DATA_PATHS['cities'], names=geonames_columns, encoding='latin-1')
        
        # Select only needed columns
        cities_df = df[['name', 'country_code', 'latitude', 'longitude', 'population', 'elevation']].copy()
        
        # Rename to match expected structure
        cities_df = cities_df.rename(columns={
            'name': 'City',
            'country_code': 'Country',
            'latitude': 'Latitude', 
            'longitude': 'Longitude',
            'population': 'Population',
            'elevation': 'Altitude' 
        })
        
        logger.info(f"GeoNames Database loaded: {len(cities_df)} records")
        logger.info(f"Cities with elevation data: {cities_df['Altitude'].notna().sum()}")
        
        return cities_df
        
    except Exception as e:
        logger.error(f"Failed to load GeoNames data: {e}")
        raise


def extract_temperature_data():
    """Load City Temperature Data"""
    try:
        logger.info("Loading City Temperature Database...")
        df = pd.read_csv(DATA_PATHS['temperature'])
        logger.info(f"Temperature Database loaded: {len(df)} records")
        
        # Display basic info
        logger.info(f"Date range: {df['Year'].min()}-{df['Year'].max()}" if 'Year' in df.columns else 'No year info')
        
        return df
    except Exception as e:
        logger.error(f"Failed to load Temperature data: {e}")
        raise


def load_to_staging(engine, world_athletics_df, cities_df, temperature_df):
    """Load raw data to staging tables with optimized performance"""
    try:
        logger.info("Loading data to staging tables...")
        
        # Method 1: Use chunking for large datasets
        logger.info(f"Loading World Athletics data ({len(world_athletics_df)} rows)...")
        world_athletics_df.to_sql(
            'raw_world_athletics', 
            engine, 
            schema='staging', 
            if_exists='replace', 
            index=False, 
            method='multi',
            chunksize=10000
        )
        logger.info("✓ World Athletics data loaded")
        
        logger.info(f"Loading Cities data ({len(cities_df)} rows)...")
        cities_df.to_sql(
            'raw_cities', 
            engine, 
            schema='staging', 
            if_exists='replace', 
            index=False, 
            method='multi',
            chunksize=25000  
        )
        logger.info("✓ Cities data loaded")
        
        logger.info(f"Loading Temperature data ({len(temperature_df)} rows)...")
        temperature_df.to_sql(
            'raw_temperature', 
            engine, 
            schema='staging', 
            if_exists='replace', 
            index=False, 
            method='multi',
            chunksize=25000
        )
        logger.info("✓ Temperature data loaded")
        
        # Verify data loading
        with engine.connect() as conn:
            athletics_count = conn.execute(text("SELECT COUNT(*) FROM staging.raw_world_athletics")).scalar()
            cities_count = conn.execute(text("SELECT COUNT(*) FROM staging.raw_cities")).scalar()
            temp_count = conn.execute(text("SELECT COUNT(*) FROM staging.raw_temperature")).scalar()
            
        logger.info(f"Data verification - Athletics: {athletics_count}, Cities: {cities_count}, Temperature: {temp_count}")
        
    except Exception as e:
        logger.error(f"Failed to load data to staging: {e}")
        raise

def main():
    """Main extraction process"""
    try:
        logger.info("Starting data extraction process...")
        
        # Create database connection
        engine = create_db_connection()
        
        # Extract raw data
        world_athletics_df = extract_world_athletics_data()
        cities_df = extract_cities_data()
        temperature_df = extract_temperature_data()
        
        # Load to staging
        load_to_staging(engine, world_athletics_df, cities_df, temperature_df)
        
        logger.info("Data extraction completed successfully!")
        
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise

if __name__ == "__main__":
    main()