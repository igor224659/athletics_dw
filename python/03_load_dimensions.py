import pandas as pd
from sqlalchemy import create_engine, text
from config import CONNECTION_STRING
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_db_connection():
    return create_engine(CONNECTION_STRING)



def load_date_dimension(engine):
    logger.info("Loading date dimension from actual performance dates...")

    # Method 1: Extract dates using pandas (more reliable)
    with engine.connect() as conn:
        query = """
        SELECT DISTINCT competition_date
        FROM reconciled.performances 
        WHERE competition_date IS NOT NULL
        """
        df = pd.read_sql(text(query), conn)

    logger.info(f"Found {len(df)} unique competition dates")

    # Parse dates using pandas (more reliable than SQL EXTRACT)
    df['competition_date_parsed'] = pd.to_datetime(df['competition_date'], errors='coerce')
    
    # Remove invalid dates
    df = df.dropna(subset=['competition_date_parsed'])
    logger.info(f"Valid dates after parsing: {len(df)}")

    # Extract date components using pandas
    df['full_date'] = df['competition_date_parsed'].dt.date
    df['year'] = df['competition_date_parsed'].dt.year
    df['month'] = df['competition_date_parsed'].dt.month
    df['quarter'] = df['competition_date_parsed'].dt.quarter
    df['month_name'] = df['competition_date_parsed'].dt.month_name()
    df['day_of_week'] = df['competition_date_parsed'].dt.day_name()

    # Add derived columns
    df['decade'] = (df['year'] // 10 * 10).astype(str) + 's'
    df['is_championship_year'] = (df['year'] % 2 == 1)  # It doesn't work for the last years (COVID messed up)
    #df['month_name'] = pd.to_datetime(df['competition_date']).dt.month_name()
    df['season'] = df['month'].apply(lambda m: 'Indoor' if m in [1,2,3,11,12] else 'Outdoor')

    # Remove duplicates and rename
    df = df.drop_duplicates(subset=['full_date'])

    final = df[['full_date', 'year', 'season', 'is_championship_year', 'decade', 'month_name', 'quarter']]

    with engine.connect() as conn:
        final.to_sql('dim_date', conn, schema='dwh', if_exists='append', index=False)
        conn.commit()
    
    logger.info(f"Date dimension loaded: {len(final)} actual dates")



def load_athlete_dimension(engine):
    logger.info("Loading athlete dimension from reconciled.athletes...")

    # Use SQL aliases to rename columns cleanly
    query = """
    SELECT 
        athlete_key,
        athlete_name_clean as athlete_name,
        nationality_standardized as nationality,
        nationality_code,
        gender,
        birth_decade,
        specialization
    FROM reconciled.athletes
    """
    
    with engine.connect() as conn:
        final = pd.read_sql(text(query), conn)

    with engine.connect() as conn:
        final.to_sql('dim_athlete', conn, schema='dwh', if_exists='append', index=False)
        conn.commit()

    logger.info(f"Athlete dimension loaded: {len(final)} records")



def load_event_dimension(engine):
    logger.info("Loading event dimension from reconciled.events...")

    # Use SQL aliases to handle column renaming cleanly
    query = """
    SELECT 
        event_key,
        event_name_standardized as event_name,
        event_category,
        event_group,
        distance_meters,
        measurement_unit,
        COALESCE(gender, 'Mixed') as gender,
        is_outdoor_event,
        world_record
    FROM reconciled.events
    """

    with engine.connect() as conn:
        final = pd.read_sql(text(query), conn)
        final.to_sql('dim_event', conn, schema='dwh', if_exists='append', index=False)
        conn.commit()

    logger.info(f"Event dimension loaded: {len(final)} records")



def load_venue_dimension(engine):
    """Load venue dimension - matches actual reconciled.venues structure"""
    logger.info("Loading venue dimension from reconciled.venues...")

    query = """
    SELECT 
        venue_key,
        venue_name_clean as venue_name,
        city_name,
        country_name,
        country_code,
        latitude,
        longitude,
        altitude,
        altitude_category,
        climate_zone
    FROM reconciled.venues
    """

    with engine.connect() as conn:
        final = pd.read_sql(text(query), conn)
        final.to_sql('dim_venue', conn, schema='dwh', if_exists='append', index=False)
        conn.commit()
        
    logger.info(f"Venue dimension loaded: {len(final)} records")



def load_weather_dimension(engine):
    logger.info("Loading weather dimension from reconciled.weather_conditions...")

    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM reconciled.weather_conditions"), conn)

    final = df[['weather_key','venue_name', 'month_name', 'temperature',
                'temperature_category', 'season_category', 'has_actual_data']]

    with engine.connect() as conn:
        final.to_sql('dim_weather', conn, schema='dwh', if_exists='append', index=False)
        conn.commit()

    logger.info(f"Weather dimension loaded: {len(final)} records")



def clear_dwh_tables(engine):
    """Clear existing data before re-loading"""
    tables = ['dim_athlete', 'dim_date', 'dim_event', 'dim_venue', 'dim_weather']
    
    with engine.connect() as conn:
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE dwh.{table} RESTART IDENTITY CASCADE"))
            logger.info(f"Cleared dwh.{table}")
        conn.commit()



def main():
    try:
        logger.info("Starting dimension loading from reconciled layer...")
        engine = create_db_connection()

        # Clear existing data first
        clear_dwh_tables(engine)

        load_date_dimension(engine)
        load_athlete_dimension(engine)
        load_event_dimension(engine)
        load_venue_dimension(engine)
        load_weather_dimension(engine)

        # Count queries
        with engine.connect() as conn:
            tables = ['dim_athlete', 'dim_date', 'dim_event', 'dim_venue', 'dim_weather']
            for table in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM dwh.{table}")).scalar()
                logger.info(f"dwh.{table}: {count} records")


        logger.info("All DWH dimensions loaded successfully.")
    except Exception as e:
        logger.error(f"Dimension loading failed: {e}")
        raise

if __name__ == "__main__":
    main()
