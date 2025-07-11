"""Configuration file for Athletics Data Warehouse Project"""

import os

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'athletics_dw',
    'username': 'athletics_user', # Username from docker-compose
    'password': 'athletics_pass123'  # Password from docker-compose
}

# Data File Paths
DATA_PATHS = {
    'world_athletics': 'data/raw/world_athletics_database.csv',
    'cities': 'data/raw/cities1000.csv',
    'temperature': 'data/raw/city_temperature.csv'
}

# Database connection string
CONNECTION_STRING = f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Data quality thresholds
DATA_QUALITY = {
    'min_result_value': 0.1,
    'max_result_value': 50000,
    'valid_genders': ['M', 'F', 'Men', 'Women'],
    'min_year': 1935,
    'max_year': 2023   
}

# Event categorization mapping
EVENT_CATEGORIES = {
    'Sprint': ['100m', '200m', '400m'],
    'Distance': ['800m', '1500m', '5000m', '10000m', 'Marathon'],
    'Hurdles': ['100m Hurdles', '110m Hurdles', '400m Hurdles'],
    'Jumps': ['Long Jump', 'High Jump', 'Triple Jump', 'Pole Vault'],
    'Throws': ['Shot Put', 'Discus Throw', 'Hammer Throw', 'Javelin Throw']
}

# Competition level mapping
COMPETITION_LEVELS = {
    'Elite': ['World Championships', 'Olympics', 'Diamond League'],
    'Professional': ['Continental Championships', 'National Championships'],
    'Amateur': ['Regional', 'Local']
}

# Altitude categories (meters)
ALTITUDE_CATEGORIES = {
    'Sea Level': (0, 500),
    'Moderate': (500, 1500), 
    'High': (1500, 3000),
    'Very High': (3000, 10000)
}

# Temperature categories (Celsius)
TEMPERATURE_CATEGORIES = {
    'Cold': (-10, 10),
    'Cool': (10, 18),
    'Moderate': (18, 24),
    'Warm': (24, 30),
    'Hot': (30, 50)
}
