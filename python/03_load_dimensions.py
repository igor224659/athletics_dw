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
    logger.info("Loading date dimension...")

    date_data = []
    for year in range(2000, 2025):
        is_championship_year = (year % 2 == 1)
        for season in ['Indoor', 'Outdoor']:
            for level in ['Elite', 'Professional', 'Amateur']:
                record = {
                    'full_date': f"{year}-{'03' if season == 'Indoor' else '07'}-15",
                    'year': year,
                    'season': season,
                    'competition_level': level,
                    'is_championship_year': is_championship_year,
                    'decade': f"{(year // 10) * 10}s",
                    'month_name': 'March' if season == 'Indoor' else 'July',
                    'quarter': 1 if season == 'Indoor' else 3
                }
                date_data.append(record)

    date_df = pd.DataFrame(date_data)
    with engine.connect() as conn:
        date_df.to_sql('dim_date', conn, schema='dwh', if_exists='append', index=False)
    
    logger.info(f"Date dimension loaded: {len(date_df)} records")



def load_athlete_dimension(engine):
    logger.info("Loading athlete dimension from reconciled.athletes...")

    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM reconciled.athletes"), conn)

    final = df[['athlete_name_clean', 'nationality_standardized', 'gender',
                'specialization', 'data_quality_score', 'source_system']].copy()
    
    with engine.connect() as conn:
        final.to_sql('dim_athlete', conn, schema='dwh', if_exists='append', index=False)

    logger.info(f"Athlete dimension loaded: {len(final)} records")



def load_event_dimension(engine):
    logger.info("Loading event dimension from reconciled.events...")

    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM reconciled.events"), conn)

    # Add placeholder fields if missing
    if 'gender' not in df.columns:
        df['gender'] = 'Mixed'
    if 'distance_meters' not in df.columns:
        df['distance_meters'] = None
    if 'world_record' not in df.columns:
        df['world_record'] = None

    final = df[['event_name_standardized', 'event_group', 'event_category',
                'measurement_unit', 'is_outdoor_event', 'gender',
                'distance_meters', 'world_record']]

    with engine.connect() as conn:
        final.to_sql('dim_event', conn, schema='dwh', if_exists='append', index=False)

    logger.info(f"Event dimension loaded: {len(final)} records")



def load_venue_dimension(engine):
    logger.info("Loading venue dimension from reconciled.venues...")

    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM reconciled.venues"), conn)

    # Ensure optional fields exist
    if 'continent' not in df.columns:
        df['continent'] = 'Unknown'
    if 'city_size' not in df.columns:
        df['city_size'] = 'Unknown'

    final = df[['venue_name_clean', 'city_name', 'country_name', 'country_code',
                'latitude', 'longitude', 'altitude', 'altitude_category',
                'continent', 'climate_zone', 'population',
                'data_quality_score', 'geographic_source']]

    with engine.connect() as conn:
        final.to_sql('dim_venue', conn, schema='dwh', if_exists='append', index=False)
    logger.info(f"Venue dimension loaded: {len(final)} records")



def load_competition_dimension(engine):
    logger.info("Loading competition dimension from reconciled.competitions...")

    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM reconciled.competitions"), conn)

    final = df[['competition_name', 'competition_type', 'competition_level',
                'prestige_level', 'is_indoor']]

    with engine.connect() as conn:
        final.to_sql('dim_competition', conn, schema='dwh', if_exists='append', index=False)
    logger.info(f"Competition dimension loaded: {len(final)} records")



def load_weather_dimension(engine):
    logger.info("Loading weather dimension from reconciled.weather_conditions...")

    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM reconciled.weather_conditions"), conn)

    # Normalizza i nomi
    df.rename(columns={
        'temperature': 'temperature_celsius',
        'month_name': 'month',
        'weather_source': 'source'
    }, inplace=True)

    final = df[['venue_name', 'month', 'temperature_celsius',
                'temperature_category', 'season_category', 'has_actual_data', 'source']]

    with engine.connect() as conn:
        final.to_sql('dim_weather', conn, schema='dwh', if_exists='append', index=False)

    logger.info(f"Weather dimension loaded: {len(final)} records")



def clear_reconciled_tables(engine):
    """Clear existing data before re-loading"""
    tables = ['dim_athlete', 'dim_competition', 'dim_date', 'dim_event', 'dim_venue', 'dim_weather']
    
    with engine.connect() as conn:
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE reconciled.{table} RESTART IDENTITY CASCADE"))
            logger.info(f"Cleared reconciled.{table}")
        conn.commit()



def main():
    try:
        logger.info("Starting dimension loading from reconciled layer...")
        engine = create_db_connection()

        # Clear existing data first
        clear_reconciled_tables(engine)

        load_date_dimension(engine)
        load_athlete_dimension(engine)
        load_event_dimension(engine)
        load_venue_dimension(engine)
        load_competition_dimension(engine)
        load_weather_dimension(engine)

        # Count queries
        with engine.connect() as conn:
            tables = ['dim_athlete', 'dim_competition', 'dim_date', 'dim_event', 'dim_venue', 'dim_weather']
            for table in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM dhw.{table}")).scalar()
                logger.info(f"dhw.{table}: {count} records")


        logger.info("All DWH dimensions loaded successfully.")
    except Exception as e:
        logger.error(f"Dimension loading failed: {e}")
        raise

if __name__ == "__main__":
    main()
