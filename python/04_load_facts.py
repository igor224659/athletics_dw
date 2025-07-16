import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import CONNECTION_STRING
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_db_connection():
    return create_engine(CONNECTION_STRING)

# Realistic World Record Thresholds Used to filter out obviously fake performances
REALISTIC_PERFORMANCE_THRESHOLDS = {
    # Men's Events - Based on current world records with 5% buffer
    'M': {
        # Sprint Events (times in seconds)
        '100 Metres': {'max_time': 11.0, 'min_time': 9.57},      # WR: 9.58, allows up to 11.0
        '200 Metres': {'max_time': 22.0, 'min_time': 19.18},     # WR: 19.19
        '400 Metres': {'max_time': 50.0, 'min_time': 43.02},     # WR: 43.03
        '300 Metres': {'max_time': 35.0, 'min_time': 30.80},         # WR: ~30.81
        '600 Metres': {'max_time': 80.0, 'min_time': 67.13},         # WR: ~67.14
        '1000 Metres': {'max_time': 170.0, 'min_time': 131.95},      # WR: ~131.96
        '2000 Metres': {'max_time': 360.0, 'min_time': 284.78},      # WR: ~284.79
        '3000 Metres': {'max_time': 550.0, 'min_time': 422.75},      # WR: ~422.76
        'Two Miles': {'max_time': 550.0, 'min_time': 472.66},        # WR: ~472.67
        '3000 Metres Steeplechase': {'max_time': 600.0, 'min_time': 486.46}, # WR: 486.47
        '5 Kilometres': {'max_time': 950.0, 'min_time': 756.0},     # WR: ~757 (road)
        '10 Kilometres': {'max_time': 1900.0, 'min_time': 1576.0},  # WR: ~1577 (road)
        '15 Kilometres': {'max_time': 3000.0, 'min_time': 2525.0},  # WR: ~2526
        '20 Kilometres': {'max_time': 4000.0, 'min_time': 3377.0},  # WR: ~3378
        'Half Marathon': {'max_time': 4500.0, 'min_time': 3668.0},  # WR: 3669 (58:01)
        '10 Miles Road': {'max_time': 3200.0, 'min_time': 2707.0},  # WR: ~2708
        '3000 Metres Race Walk': {'max_time': 900.0, 'min_time': 665.0},    # WR: ~666
        '5000 Metres Race Walk': {'max_time': 1400.0, 'min_time': 1134.0},  # WR: ~1135
        '10000 Metres Race Walk': {'max_time': 2700.0, 'min_time': 2316.0}, # WR: ~2317
        '20000 Metres Race Walk': {'max_time': 5400.0, 'min_time': 4666.0}, # WR: ~4667
        '5 Kilometres Race Walk': {'max_time': 1400.0, 'min_time': 1134.0}, # WR: ~1135
        '10 Kilometres Race Walk': {'max_time': 2700.0, 'min_time': 2316.0}, # WR: ~2317
        '20 Kilometres Race Walk': {'max_time': 5400.0, 'min_time': 4666.0}, # WR: ~4667
        '30 Kilometres Race Walk': {'max_time': 8200.0, 'min_time': 7199.0}, # WR: ~7200
        '35 Kilometres Race Walk': {'max_time': 9600.0, 'min_time': 8399.0}, # WR: ~8400
        '50 Kilometres Race Walk': {'max_time': 14000.0, 'min_time': 12396.0}, # WR: ~12397
        '800 Metres': {'max_time': 130.0, 'min_time': 100.90},    # WR: 100.91
        '1500 Metres': {'max_time': 260.0, 'min_time': 205.99},  # WR: 206.00
        'One Mile': {'max_time': 280.0, 'min_time': 223.12},     # WR: 223.13
        '5000 Metres': {'max_time': 900.0, 'min_time': 757.34},  # WR: 757.35
        '10000 Metres': {'max_time': 1800.0, 'min_time': 1577.52}, # WR: 1577.53
        'Marathon': {'max_time': 9000.0, 'min_time': 7298.0},   # WR: 7299 (2:01:09)
        '110 Metres Hurdles': {'max_time': 15.0, 'min_time': 12.79}, # WR: 12.80
        '400 Metres Hurdles': {'max_time': 55.0, 'min_time': 45.93}, # WR: 45.94
        'High Jump': {'max_distance': 2.46, 'min_distance': 1.5},     # WR: 2.45
        'Long Jump': {'max_distance': 8.96, 'min_distance': 5.0},     # WR: 8.95
        'Triple Jump': {'max_distance': 18.30, 'min_distance': 10.0}, # WR: 18.29
        'Pole Vault': {'max_distance': 6.24, 'min_distance': 3.0},    # WR: 6.23
        'Shot Put': {'max_distance': 23.38, 'min_distance': 8.0},     # WR: 23.37
        'Discus Throw': {'max_distance': 74.09, 'min_distance': 30.0}, # WR: 74.08
        'Hammer Throw': {'max_distance': 86.75, 'min_distance': 40.0}, # WR: 86.74
        'Javelin Throw': {'max_distance': 98.49, 'min_distance': 40.0}, # WR: 98.48
    },
    
    # Women's Events - Based on current world records with 5% buffer
    'F': {
        # Sprint Events (times in seconds)
        '100 Metres': {'max_time': 12.0, 'min_time': 10.48},      # WR: 10.49
        '200 Metres': {'max_time': 24.0, 'min_time': 21.33},     # WR: 21.34
        '400 Metres': {'max_time': 55.0, 'min_time': 47.59},     # WR: 47.60
        '300 Metres': {'max_time': 38.0, 'min_time': 34.13},         # WR: ~34.14
        '600 Metres': {'max_time': 90.0, 'min_time': 73.24},         # WR: ~73.25
        '1000 Metres': {'max_time': 185.0, 'min_time': 149.33},      # WR: ~149.34
        '2000 Metres': {'max_time': 400.0, 'min_time': 318.57},      # WR: ~318.58
        '3000 Metres': {'max_time': 620.0, 'min_time': 486.10},      # WR: ~486.11
        'One Mile': {'max_time': 300.0, 'min_time': 252.55},         # WR: 252.56
        'Two Miles': {'max_time': 620.0, 'min_time': 540.32},        # WR: ~540.33
        '3000 Metres Steeplechase': {'max_time': 700.0, 'min_time': 558.77}, # WR: 558.78
        '5 Kilometres': {'max_time': 1050.0, 'min_time': 850.0},    # WR: ~851 (road)
        '10 Kilometres': {'max_time': 2150.0, 'min_time': 1771.0},  # WR: ~1772 (road)
        '15 Kilometres': {'max_time': 3300.0, 'min_time': 2812.0},  # WR: ~2813
        '20 Kilometres': {'max_time': 4500.0, 'min_time': 3899.0},  # WR: ~3900
        'Half Marathon': {'max_time': 5100.0, 'min_time': 4078.0},  # WR: 4079 (1:04:39)
        '10 Miles Road': {'max_time': 3600.0, 'min_time': 3049.0},  # WR: ~3050
        '3000 Metres Race Walk': {'max_time': 1000.0, 'min_time': 749.0},   # WR: ~750
        '5000 Metres Race Walk': {'max_time': 1600.0, 'min_time': 1288.0},  # WR: ~1289
        '10000 Metres Race Walk': {'max_time': 3200.0, 'min_time': 2643.0}, # WR: ~2644
        '20000 Metres Race Walk': {'max_time': 6500.0, 'min_time': 5466.0}, # WR: ~5467
        '5 Kilometres Race Walk': {'max_time': 1600.0, 'min_time': 1288.0}, # WR: ~1289
        '10 Kilometres Race Walk': {'max_time': 3200.0, 'min_time': 2643.0}, # WR: ~2644
        '20 Kilometres Race Walk': {'max_time': 6500.0, 'min_time': 5466.0}, # WR: ~5467
        '35 Kilometres Race Walk': {'max_time': 11500.0, 'min_time': 10199.0}, # WR: ~10200
        '50 Kilometres Race Walk': {'max_time': 16500.0, 'min_time': 14413.0}, # WR: ~14414
        '800 Metres': {'max_time': 140.0, 'min_time': 113.27},   # WR: 113.28
        '1500 Metres': {'max_time': 280.0, 'min_time': 230.06},  # WR: 230.07
        'One Mile': {'max_time': 300.0, 'min_time': 252.55},     # WR: 252.56
        '5000 Metres': {'max_time': 1000.0, 'min_time': 851.14}, # WR: 851.15
        '10000 Metres': {'max_time': 2100.0, 'min_time': 1771.77}, # WR: 1771.78
        'Marathon': {'max_time': 10800.0, 'min_time': 8168},  # WR: 8169 (2:14:04)
        '100 Metres Hurdles': {'max_time': 15.0, 'min_time': 12.19}, # WR: 12.20
        '400 Metres Hurdles': {'max_time': 60.0, 'min_time': 50.67}, # WR: 50.68
        'High Jump': {'max_distance': 2.10, 'min_distance': 1.3},     # WR: 2.09
        'Long Jump': {'max_distance': 7.53, 'min_distance': 4.5},     # WR: 7.52
        'Triple Jump': {'max_distance': 15.75, 'min_distance': 9.0},  # WR: 15.74
        'Pole Vault': {'max_distance': 5.07, 'min_distance': 2.5},    # WR: 5.06
        'Shot Put': {'max_distance': 22.64, 'min_distance': 7.0},     # WR: 22.63 (4kg)
        'Discus Throw': {'max_distance': 76.81, 'min_distance': 25.0}, # WR: 76.80 (1kg)
        'Hammer Throw': {'max_distance': 82.99, 'min_distance': 35.0}, # WR: 82.98 (4kg)
        'Javelin Throw': {'max_distance': 72.29, 'min_distance': 25.0}, # WR: 72.28 (600g)
    }
}

def is_realistic_performance(result_value, event_name, measurement_unit, gender):
    """
    Check if a performance result is realistic based on world record thresholds
    """
    try:
        if pd.isna(result_value) or pd.isna(event_name) or result_value <= 0:
            return False
        
        # Normalize gender
        gender_key = 'M' if str(gender).upper() in ['M', 'MALE', 'MEN'] else 'F'
        
        # Clean event name for matching
        event_str = str(event_name).strip()
        
        # Get thresholds for this gender and event
        if gender_key not in REALISTIC_PERFORMANCE_THRESHOLDS:
            return True  # If no gender info, don't filter
        
        gender_thresholds = REALISTIC_PERFORMANCE_THRESHOLDS[gender_key]
        
        # Find matching event thresholds
        event_thresholds = None
        
        # Exact match first
        if event_str in gender_thresholds:
            event_thresholds = gender_thresholds[event_str]
        else:
            # Partial match for event variations
            for threshold_event, thresholds in gender_thresholds.items():
                if threshold_event.lower() in event_str.lower() or event_str.lower() in threshold_event.lower():
                    event_thresholds = thresholds
                    break
        
        if not event_thresholds:
            return True  # If no thresholds found, don't filter (conservative approach)
        
        # Apply thresholds based on measurement unit
        if measurement_unit == 'seconds':
            # For time events: check if time is within realistic range
            max_time = event_thresholds.get('max_time')
            min_time = event_thresholds.get('min_time')
            
            if max_time and result_value > max_time:
                return False  # Too slow
            if min_time and result_value < min_time:
                return False  # Too fast (impossible)
                
        else:  # meters
            # For distance/height events: check if distance is within realistic range
            max_distance = event_thresholds.get('max_distance')
            min_distance = event_thresholds.get('min_distance')
            
            if max_distance and result_value > max_distance:
                return False  # Too far (impossible)
            if min_distance and result_value < min_distance:
                return False  # Too short (likely error)
        
        return True  # Performance passes all checks
        
    except Exception as e:
        logger.warning(f"Error validating performance for {event_name} ({gender}): {e}")
        return True  # Conservative: don't filter if validation fails


def filter_performance_outliers(df):
    """
    Filter out unrealistic performances before calculating performance scores
    """
    logger.info("=== FILTERING PERFORMANCE OUTLIERS ===")
    
    initial_count = len(df)
    
    # Apply realistic performance filter
    df['is_realistic'] = df.apply(
        lambda row: is_realistic_performance(
            row['result_value'], 
            row['event_name'], 
            row['measurement_unit'], 
            row['gender']
        ), axis=1
    )
    
    # Separate realistic and outlier performances
    realistic_df = df[df['is_realistic']].copy()
    outliers_df = df[~df['is_realistic']].copy()
    
    # Log filtering results
    outliers_count = len(outliers_df)
    outliers_percentage = (outliers_count / initial_count) * 100
    
    logger.info(f"Initial performances: {initial_count:,}")
    logger.info(f"Realistic performances: {len(realistic_df):,}")
    logger.info(f"Filtered outliers: {outliers_count:,} ({outliers_percentage:.1f}%)")
    
    # Analyze outliers by event
    if len(outliers_df) > 0:
        outlier_analysis = outliers_df.groupby(['event_name', 'gender']).agg({
            'result_value': ['count', 'min', 'max', 'mean']
        }).round(3)
        
        logger.info("TOP OUTLIER EVENTS:")
        for event_gender, stats in outlier_analysis.head(10).iterrows():
            event_name, gender = event_gender
            count = stats[('result_value', 'count')]
            min_val = stats[('result_value', 'min')]
            max_val = stats[('result_value', 'max')]
            logger.info(f"  {event_name} ({gender}): {count} outliers, range: {min_val}-{max_val}")
    
    logger.info("======================================")
    
    return realistic_df.drop(columns=['is_realistic'])


# World Athletics coefficients for scientific performance scoring
WORLD_ATHLETICS_COEFFICIENTS = {
    # MEN'S EVENTS
    'M': {
        # Track events (time-based)
        '100 Metres': {'A': 28.67, 'B': 18.0, 'C': 1.81},
        '200 Metres': {'A': 6.674, 'B': 38.0, 'C': 1.81},
        '400 Metres': {'A': 1.745, 'B': 82.0, 'C': 1.81},
        '800 Metres': {'A': 0.1444, 'B': 254.0, 'C': 1.81},
        '1500 Metres': {'A': 0.0504, 'B': 480.0, 'C': 1.81},
        '5000 Metres': {'A': 0.00283, 'B': 2100.0, 'C': 1.81},
        '10000 Metres': {'A': 0.0008436, 'B': 4200.0, 'C': 1.81},
        
        # Hurdles and barriers
        '110 Metres Hurdles': {'A': 6.544, 'B': 28.5, 'C': 1.92},
        '400 Metres Hurdles': {'A': 0.8722, 'B': 95.5, 'C': 1.88},
        '3000 Metres Steeplechase': {'A': 0.004711, 'B': 1254.0, 'C': 1.88},

        # Field events (distance/height-based)
        'High Jump': {'A': 625.1, 'B': 0.75, 'C': 1.4},
        'Long Jump': {'A': 79.42, 'B': 1.4, 'C': 1.4},
        'Triple Jump': {'A': 27.37, 'B': 2.5, 'C': 1.4},
        'Pole Vault': {'A': 142.2, 'B': 1.0, 'C': 1.35},
        'Shot Put': {'A': 51.8, 'B': 1.5, 'C': 1.05},
        'Discus Throw': {'A': 12.28, 'B': 4.0, 'C': 1.1},
        'Hammer Throw': {'A': 13.17, 'B': 7.0, 'C': 1.05},
        'Javelin Throw': {'A': 7.58, 'B': 7.0, 'C': 1.15},
        
        # Additional track events (estimated A values)
        '300 Metres': {'A': 2.139, 'B': 65.0, 'C': 1.81},           # Rarely run
        '600 Metres': {'A': 0.2414, 'B': 185.0, 'C': 1.81},         # Middle distance
        '1000 Metres': {'A': 0.0601, 'B': 375.0, 'C': 1.81},        # Middle distance
        '2000 Metres': {'A': 0.02174, 'B': 720.0, 'C': 1.81},       # Distance
        '3000 Metres': {'A': 0.007944, 'B': 1200.0, 'C': 1.81},     # Distance
        'One Mile': {'A': 0.04907, 'B': 500.0, 'C': 1.81},          # 1609m equivalent
        'Two Miles': {'A': 0.013154, 'B': 1050.0, 'C': 1.81},        # ~3200m

        # Short Road Events
        '5 Kilometres': {'A': 0.002768, 'B': 2100.0, 'C': 1.81},      
        '10 Kilometres': {'A': 0.0008375, 'B': 4200.0, 'C': 1.81},    
        '15 Kilometres': {'A': 0.000407, 'B': 6300.0, 'C': 1.81},       # ~45min baseline
        '20 Kilometres': {'A': 0.0002434, 'B': 8400.0, 'C': 1.81},       # ~70min baseline
        
        # Long Road Events  
        'Half Marathon': {'A': 0.00143, 'B': 5400.0, 'C': 1.81},     # 1.5 hours baseline
        'Marathon': {'A': 0.0004865, 'B': 10800.0, 'C': 1.81},         # 3 hours baseline
        '10 Miles Road': {'A': 0.0003, 'B': 7200.0, 'C': 1.81},        # ~2 hours baseline
        
        # Track Race Walking
        '3000 Metres Race Walk': {'A': 0.003503, 'B': 1800.0, 'C': 1.81},    # ~30min baseline
        '5000 Metres Race Walk': {'A': 0.001396, 'B': 3000.0, 'C': 1.81},    # ~50min baseline
        '10000 Metres Race Walk': {'A': 0.0004205, 'B': 6000.0, 'C': 1.81},   # ~100min baseline
        '20000 Metres Race Walk': {'A': 0.0001251, 'B': 12000.0, 'C': 1.81},  # ~200min baseline
        
        # Road Race Walking
        '5 Kilometres Race Walk': {'A': 0.00139, 'B': 3000.0, 'C': 1.81},   # ~50min baseline
        '10 Kilometres Race Walk': {'A': 0.0004214, 'B': 6000.0, 'C': 1.81},  # ~100min baseline
        '20 Kilometres Race Walk': {'A': 0.0001255, 'B': 12000.0, 'C': 1.81}, # ~200min baseline
        '30 Kilometres Race Walk': {'A': 0.0000634, 'B': 18000.0, 'C': 1.81}, # ~300min baseline
        '35 Kilometres Race Walk': {'A': 0.00004824, 'B': 21000.0, 'C': 1.81}, # ~350min baseline
        '50 Kilometres Race Walk': {'A': 0.00002723, 'B': 30000.0, 'C': 1.81}, # ~500min baseline
        
    },
    
    # WOMEN'S EVENTS
    'F': {
        # Track events (time-based)
        '100 Metres': {'A': 18.6, 'B': 21.0, 'C': 1.81},
        '200 Metres': {'A': 5.217, 'B': 42.5, 'C': 1.81},
        '400 Metres': {'A': 1.377, 'B': 91.7, 'C': 1.81},
        '800 Metres': {'A': 0.11594, 'B': 285.0, 'C': 1.81},
        '1500 Metres': {'A': 0.04106, 'B': 535.0, 'C': 1.81},
        '5000 Metres': {'A': 0.00213, 'B': 2400.0, 'C': 1.81},
        '10000 Metres': {'A': 0.00064, 'B': 4800.0, 'C': 1.81},
        
        # Hurdles
        '100 Metres Hurdles': {'A': 9.31, 'B': 26.7, 'C': 1.835},  # Women's hurdles
        '400 Metres Hurdles': {'A': 0.671, 'B': 107.0, 'C': 1.88},
        '3000 Metres Steeplechase': {'A': 0.003303, 'B': 1465.0, 'C': 1.88},
        
        # Additional track events
        '300 Metres': {'A': 1.789, 'B': 72.0, 'C': 1.81},
        '600 Metres': {'A': 0.1903, 'B': 210.0, 'C': 1.81},
        '800 Metres': {'A': 0.115936, 'B': 285.0, 'C': 1.81},
        '1000 Metres': {'A': 0.04778, 'B': 425.0, 'C': 1.81},
        '2000 Metres': {'A': 0.01626, 'B': 820.0, 'C': 1.81},
        '3000 Metres': {'A': 0.005886, 'B': 1380.0, 'C': 1.81},
        'One Mile': {'A': 0.03708, 'B': 570.0, 'C': 1.81},
        'Two Miles': {'A': 0.00972, 'B': 1200.0, 'C': 1.81},

        # Short Road Events
        '5 Kilometres': {'A': 0.002119, 'B': 2400.0, 'C': 1.81},        # Same as 5000m
        '10 Kilometres': {'A': 0.000641, 'B': 4800.0, 'C': 1.81},       # Same as 10000m
        '15 Kilometres': {'A': 0.0003061, 'B': 7200.0, 'C': 1.81},       # ~54min baseline
        '20 Kilometres': {'A': 0.0001841, 'B': 9600.0, 'C': 1.81},       # ~80min baseline
        
        # Long Road Events
        'Half Marathon': {'A': 0.0008882, 'B': 6300.0, 'C': 1.81},       # 1.75 hours baseline
        'Marathon': {'A': 0.00029305, 'B': 12600.0, 'C': 1.81},          # 3.5 hours baseline
        '10 Miles Road': {'A': 0.0002112, 'B': 8400.0, 'C': 1.81},       # ~2.3 hours baseline
        
        # Track Race Walking
        '3000 Metres Race Walk': {'A': 0.0024365, 'B': 2100.0, 'C': 1.81},    # ~35min baseline
        '5000 Metres Race Walk': {'A': 0.00093, 'B': 3600.0, 'C': 1.81},     # ~60min baseline
        '10000 Metres Race Walk': {'A': 0.0002728, 'B': 7200.0, 'C': 1.81},   # ~120min baseline
        '20000 Metres Race Walk': {'A': 0.00008003, 'B': 14400.0, 'C': 1.81}, # ~240min baseline
        
        # Road Race Walking
        '5 Kilometres Race Walk': {'A': 0.000934, 'B': 3600.0, 'C': 1.81},    # ~60min baseline
        '10 Kilometres Race Walk': {'A': 0.0002733, 'B': 7200.0, 'C': 1.81},  # ~120min baseline
        '20 Kilometres Race Walk': {'A': 0.0000808, 'B': 14400.0, 'C': 1.81}, # ~240min baseline
        '35 Kilometres Race Walk': {'A': 0.00003187, 'B': 25200.0, 'C': 1.81}, # ~420min baseline
        '50 Kilometres Race Walk': {'A': 0.00001765, 'B': 36000.0, 'C': 1.81}, # ~600min baseline
        
        # Field events for women (different standards)
        'High Jump': {'A': 869.0, 'B': 0.75, 'C': 1.4},           # Adjusted for women's records
        'Long Jump': {'A': 105.53, 'B': 1.4, 'C': 1.4},            # Adjusted for women's records  
        'Triple Jump': {'A': 34.86, 'B': 2.5, 'C': 1.4},           # Adjusted for women's records
        'Pole Vault': {'A': 194.6, 'B': 1.0, 'C': 1.35},          # Adjusted for women's records
        'Shot Put': {'A': 55.75, 'B': 1.5, 'C': 1.05},             # Different implement weight
        'Discus Throw': {'A': 12.18, 'B': 3.0, 'C': 1.1},         # Different implement weight
        'Hammer Throw': {'A': 13.26, 'B': 4.0, 'C': 1.05},         # Different implement weight
        'Javelin Throw': {'A': 9.983, 'B': 3.0, 'C': 1.15},        # Different implement specs
    }
}

def calculate_performance_score_enhanced(result, event_name, measurement_unit, gender):
    """
    Enhanced performance score calculation with gender-specific coefficients
    
    Args:
        result: Performance result (time in seconds or distance in meters)
        event_name: Name of the athletic event
        measurement_unit: 'seconds' for time events, 'meters' for distance/height events
        gender: 'M', 'F', 'Male', 'Female', 'Men', 'Women', etc.
    
    Returns:
        Performance score (0-1400 scale, with elite performances around 1000-1200)
    """
    try:
        if pd.isna(result) or pd.isna(event_name) or result <= 0:
            return 500.0  # Default score for invalid data
        
        # Clean event name for matching
        event_str = str(event_name).strip()
        
        # Try to find coefficients for this gender and event
        coeffs = None
        gender_coeffs = WORLD_ATHLETICS_COEFFICIENTS[gender]
        
        # Exact match first
        if event_str in gender_coeffs:
            coeffs = gender_coeffs[event_str]
        else:
            # Partial match - look for event keywords
            for wa_event, wa_coeffs in gender_coeffs.items():
                if wa_event.lower() in event_str.lower() or event_str.lower() in wa_event.lower():
                    coeffs = wa_coeffs
                    break
        
        if coeffs:
            # Apply World Athletics formula
            A, B, C = coeffs['A'], coeffs['B'], coeffs['C']
            
            try:
                if measurement_unit == 'seconds':
                    # For time events: better time (lower) = higher score
                    # Formula: A × |B - T|^C where T is time
                    if result <= 0:
                        return 0.0
                    score = A * pow(abs(B - result), C)
                else:
                    # For distance/height events: better distance (higher) = higher score  
                    # Formula: A × |T - B|^C where T is distance/height
                    if result <= B:  # Performance must exceed baseline
                        return max(0.0, A * pow(abs(result - B), C) * 0.1)  # Minimal score for sub-baseline
                    score = A * pow(abs(result - B), C)
                
                # Apply reasonable bounds (World Athletics scale typically 0-1400)
                return max(0.0, min(1400.0, score))
                
            except (ValueError, OverflowError, ZeroDivisionError) as e:
                logger.warning(f"Mathematical error in World Athletics formula for {event_name} ({gender}): {e}")
                return 500.0  # Fallback score
        
        else:
            # No coefficients found - use legacy calculation
            logger.info(f"No World Athletics coefficients found for {event_name} ({gender})")
            #return calculate_legacy_score(result, event_str, measurement_unit, gender)
            
    except Exception as e:
        logger.warning(f"Error calculating performance score for {event_name} ({gender}): {e}")
        return 500.0


# Event categorization for environmental calculations
EVENT_CATEGORIES = {
    'Sprint': ['100 Metres', '200 Metres', '300 Metres', '400 Metres', '100 Metres Hurdles', '110 Metres Hurdles', '400 Metres Hurdles'],
    'Middle Distance': ['600 Metres', '800 Metres', '1000 Metres', '1500 Metres', 'One Mile', 'Two Miles', '3000 Metres', '3000 Metres Steeplechase', '2000 Metres', '2000 Metres Steeplechase'],
    'Distance': ['5000 Metres', '10000 Metres', 'Marathon', 'Half Marathon', '3000 Metres Race Walk', '5000 Metres Race Walk', '10000 Metres Race Walk', '20000 Metres Race Walk'],
    'Jumps': ['High Jump', 'Long Jump', 'Triple Jump', 'Pole Vault'],
    'Throws': ['Shot Put', 'Discus Throw', 'Hammer Throw', 'Javelin Throw']
}


def get_event_duration_category(event_name):
    """Get event category for environmental adjustments"""
    if pd.isna(event_name):
        return 'Field'
    
    event_str = str(event_name).strip()
    for category, events in EVENT_CATEGORIES.items():
        if any(event.lower() in event_str.lower() for event in events):
            return category
    return 'Field'  # Default



def calculate_temperature_impact_factor(temperature, event_name):
    """Calculate temperature impact factor using scientific research"""
    try:
        if pd.isna(temperature):
            return 1.0  # Neutral impact
        
        # Optimal temperature range: 7-15°C (research-based)
        optimal_temp = 11.0  # Middle of optimal range
        temp_deviation = abs(temperature - optimal_temp)
        
        # FIXED: Use event_name instead of event_category
        duration_category = get_event_duration_category(event_name)
        
        # Impact rates based on event duration (scientific research)
        if duration_category in ['Sprint', 'Jumps', 'Throws']:
            # Short events: minimal temperature impact
            impact_rate = 0.001  # 0.1% per degree deviation
        elif duration_category == 'Middle Distance':
            # Medium events: moderate impact
            impact_rate = 0.002  # 0.2% per degree deviation
        elif duration_category == 'Distance':
            # Long events: significant impact  
            impact_rate = 0.004  # 0.4% per degree deviation
        else:
            # Default for track events
            impact_rate = 0.002
        
        # Calculate impact factor
        # For hot conditions (temp > optimal): performance decreases
        # For cold conditions (temp < optimal): performance also decreases
        impact_factor = 1.0 - (temp_deviation * impact_rate)
        
        # Apply reasonable bounds (minimum 0.5, maximum 1.5)
        return max(0.5, min(1.5, impact_factor))
        
    except Exception as e:
        logger.warning(f"Error calculating temperature impact for {event_name} at {temperature}°C: {e}")
        return 1.0



def calculate_performance_score(result, event_name, unit):
    """Calculate performance score using World Athletics standards"""
    try:
        if pd.isna(result) or pd.isna(event_name):
            return 500.0  # Default score
        
        event_str = str(event_name).strip()
        
        # Try to find exact match in World Athletics coefficients
        coeffs = None
        for wa_event, wa_coeffs in WORLD_ATHLETICS_COEFFICIENTS.items():
            if wa_event.lower() in event_str.lower():
                coeffs = wa_coeffs
                break
        
        if coeffs:
            # Use World Athletics formula: Points = A × |B - T|^C (for time) or A × |T - B|^C (for distance)
            A, B, C = coeffs['A'], coeffs['B'], coeffs['C']
            
            try:
                if unit == 'seconds':
                    # For time events: better time = higher score
                    if result <= 0:
                        return 0.0
                    score = A * pow(abs(B - result), C)
                else:
                    # For distance/height events: better distance = higher score
                    if result <= B:  # Performance must exceed baseline
                        return 0.0
                    score = A * pow(abs(result - B), C)
                
                # Apply reasonable bounds
                return max(0.0, min(1400.0, score))
                
            except (ValueError, OverflowError) as e:
                logger.warning(f"Mathematical error in World Athletics formula for {event_name}: {e}")
                # Fall through to legacy calculation
        
        # Legacy calculation for events not in World Athletics table
        if unit == 'seconds':
            if '100m' in event_str.lower():
                return max(0, 1000 - (result - 9.5) * 200)
            elif '200m' in event_str.lower():
                return max(0, 1000 - (result - 19.0) * 100)
            elif 'mile' in event_str.lower():
                return max(0, 1000 - (result - 220) * 5)
            elif 'marathon' in event_str.lower():
                return max(0, 1000 - (result - 7260) * 0.5)
            else:
                return max(0, 1000 - result * 2)
        else:  # meters
            if 'shot put' in event_str.lower():
                return min(1000, result * 50)
            elif 'javelin' in event_str.lower():
                return min(1000, result * 12.5)
            elif 'high jump' in event_str.lower():
                return min(1000, result * 435)
            elif 'long jump' in event_str.lower():
                return min(1000, result * 118)
            else:
                return min(1000, result * 25)
                
    except Exception as e:
        logger.warning(f"Error calculating performance score for {event_name}: {e}")
        return 500.0



def calculate_altitude_adjustment(result, altitude, event_name, measurement_unit):
    """
    Calculate what this performance would be at sea level (300m baseline)
    
    LOGIC: Normalize all performances to sea level for fair comparison
    
    For time-based events:
    - At altitude, endurance events are SLOWER (less oxygen) → sea level equivalent is FASTER
    - At altitude, sprint events are FASTER (less air resistance) → sea level equivalent is SLOWER
    
    For distance/height events:
    - At altitude, throws go FURTHER (less air resistance) → sea level equivalent is SHORTER
    - At altitude, jumps go HIGHER/FURTHER (less air resistance) → sea level equivalent is LOWER
    """
    try:
        if pd.isna(altitude) or altitude <= 300:  # No adjustment below 300m baseline
            return result
        
        duration_category = get_event_duration_category(event_name)
        altitude_km = (altitude - 300) / 1000.0  # Altitude above 300m baseline
        
        # Scientific altitude effects (research-based coefficients)
        if duration_category in ['Middle Distance', 'Distance']:
            # ENDURANCE EVENTS: Penalized by altitude due to reduced oxygen
            # Actual performance at altitude is WORSE (slower time)
            # Sea level equivalent would be BETTER (faster time)
            time_penalty_per_1000m = 0.063  # 6.3% time penalty per 1000m altitude
            penalty_factor = 1.0 + (altitude_km * time_penalty_per_1000m)
            sea_level_equivalent = result / penalty_factor  # Remove the altitude penalty
            
        elif duration_category == 'Sprint':
            # SPRINT EVENTS: Helped by altitude due to reduced air resistance
            # Actual performance at altitude is BETTER (faster time)  
            # Sea level equivalent would be WORSE (slower time)
            time_benefit_per_1000m = 0.0095  # 0.95% time improvement per 1000m altitude
            benefit_factor = 1.0 + (altitude_km * time_benefit_per_1000m)
            sea_level_equivalent = result * benefit_factor  # Remove the altitude benefit
            
        elif duration_category == 'Throws':
            # THROWING EVENTS: Helped by altitude due to reduced air resistance
            # Actual performance at altitude is BETTER (longer distance)
            # Sea level equivalent would be WORSE (shorter distance)
            distance_benefit_per_1000m = 0.012  # 1.2% distance improvement per 1000m
            benefit_factor = 1.0 + (altitude_km * distance_benefit_per_1000m)
            sea_level_equivalent = result / benefit_factor  # Remove the altitude benefit
            
        elif duration_category == 'Jumps':
            # JUMPING EVENTS: Helped by altitude (reduced air resistance + slightly less gravity)
            # Actual performance at altitude is BETTER (higher/longer jump)
            # Sea level equivalent would be WORSE (lower distance)
            jump_benefit_per_1000m = 0.008  # 0.8% improvement per 1000m altitude
            benefit_factor = 1.0 + (altitude_km * jump_benefit_per_1000m)
            sea_level_equivalent = result / benefit_factor  # Remove the altitude benefit
            
        else:
            # Unknown event category - no adjustment
            sea_level_equivalent = result
        
        # Apply reasonable bounds for DECIMAL(10,3)
        return max(0.0, min(999999.0, sea_level_equivalent))
        
    except Exception as e:
        logger.warning(f"Error calculating altitude adjustment for {event_name}: {e}")
        return result
    
    


def load_fact_table(engine):
    """
    FACT LOADING - 5 Essential Dimensions Only
    Focuses on environmental impact analysis without competition complexity
    
    Dimensions:
    1. WHO - Athlete (athlete_key)
    2. WHAT - Event (event_key) 
    3. WHERE - Venue (venue_key)
    4. WHEN - Date (date_key)
    5. CONDITIONS - Weather (weather_key)
    
    Grain: One performance per athlete/event/venue/date/weather combination
    """
    logger.info("FACT LOADING - 5 Essential Dimensions")


    # Step 1: Load performances (no competition_id needed)
    with engine.connect() as conn:
        perf_query = """
        SELECT 
            athlete_key, event_key, venue_key, weather_key,
            competition_date, result_value, wind_reading, position_finish,
            data_source, data_quality_score, created_date
        FROM reconciled.performances p
        WHERE result_value IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM reconciled.events e 
            WHERE e.event_key = p.event_key 
            AND e.event_name ILIKE '%half marathon%'
        )
        """
        perf = pd.read_sql(text(perf_query), conn)

    logger.info(f"Loaded {len(perf)} performance records from reconciled layer")


    # Step 2: Load the 5 essential dimensions
    with engine.connect() as conn:
        # WHO - Athlete
        athlete_dim = pd.read_sql(text("""
            SELECT athlete_key, athlete_name, nationality_code, gender
            FROM dwh.dim_athlete
        """), conn)
        
        # WHAT - Event  
        event_dim = pd.read_sql(text("""
            SELECT event_key, event_name, event_category, measurement_unit, distance_meters
            FROM dwh.dim_event  
        """), conn)
        
        # WHERE - Venue
        #WHERE altitude IS NOT NULL 
        #    AND altitude >= 0 
        #    AND altitude <= 4000
        venue_dim = pd.read_sql(text("""
            SELECT venue_key, venue_name, city_name, country_code, altitude, climate_zone
            FROM dwh.dim_venue
            WHERE altitude IS NOT NULL 
            AND altitude >= 0 
            AND altitude <= 4000
        """), conn)
        
        # CONDITIONS - Weather
        weather_dim = pd.read_sql(text("""
            SELECT weather_key, venue_name, month_name, temperature
            FROM dwh.dim_weather
        """), conn)

    logger.info("Loaded 5 essential dimensions:")
    logger.info(f"Athletes: {len(athlete_dim)} records")
    logger.info(f"Events: {len(event_dim)} records")
    logger.info(f"Venues: {len(venue_dim)} records")
    logger.info(f"Weather: {len(weather_dim)} records")


    # Step 3: Handle date dimension (WHEN)
    perf['competition_date_parsed'] = pd.to_datetime(perf['competition_date'], errors='coerce')
    
    with engine.connect() as conn:
        date_dim = pd.read_sql(text("""
            SELECT date_key, full_date, year, season, decade
            FROM dwh.dim_date
        """), conn)
    
    date_dim['full_date'] = pd.to_datetime(date_dim['full_date'])
    logger.info(f"Dates: {len(date_dim)} records")


    # Step 4: Join the 5 dimensions (simplified joins)
    logger.info("Joining 5 essential dimensions...")
    
    # WHO - Athlete  
    perf = perf.merge(
        athlete_dim[['athlete_key', 'athlete_name', 'nationality_code', 'gender']], 
        on='athlete_key', how='left'
    )
    
    # WHAT - Event
    perf = perf.merge(
        event_dim[['event_key', 'event_name', 'event_category', 'measurement_unit']], 
        on='event_key', how='left'
    )
    
    # WHERE - Venue
    perf = perf.merge(
        venue_dim[['venue_key', 'venue_name', 'altitude', 'climate_zone']], 
        on='venue_key', how='inner'  # INNER JOIN removes performances without altitude
    )
    
    # CONDITIONS - Weather
    perf = perf.merge(
        weather_dim[['weather_key', 'temperature']], 
        on='weather_key', how='left'
    )
    
    # WHEN - Date
    perf = perf.merge(
        date_dim[['date_key', 'full_date']], 
        left_on='competition_date_parsed', right_on='full_date', how='left'
    )

    logger.info(f"After joining 5 dimensions: {len(perf)} records")

    # Step 5: Check mapping success rates
    key_success = {
        'athlete_key': (~perf['athlete_key'].isna()).sum(),
        'event_key': (~perf['event_key'].isna()).sum(),
        'venue_key': (~perf['venue_key'].isna()).sum(),
        'date_key': (~perf['date_key'].isna()).sum(),
        'weather_key': (~perf['weather_key'].isna()).sum()
    }
    
    logger.info("5-Dimension join success rates:")
    for key, count in key_success.items():
        success_rate = (count / len(perf)) * 100
        logger.info(f"  {key}: {count}/{len(perf)} ({success_rate:.1f}%)")


    # Step 6: Filter out records missing critical dimensions
    initial_count = len(perf)
    perf = perf.dropna(subset=['athlete_key', 'event_key'])
    logger.info(f"Filtered out {initial_count - len(perf)} records missing critical dimensions")


    # Step 7: OUTLIER FILTERING - Remove fake/unrealistic performances
    perf_filtered = filter_performance_outliers(perf)

    outliers_removed = len(perf) - len(perf_filtered)
    logger.info(f"Outlier filtering removed {outliers_removed} suspicious performances")

    # Step 8: Final data quality filtering
    initial_count = len(perf_filtered)
    perf_filtered = perf_filtered.dropna(subset=['athlete_key', 'event_key', 'venue_key'])
    logger.info(f"Filtered out {initial_count - len(perf_filtered)} records missing critical dimensions")

    perf = perf_filtered


    # Step 9: Calculate ALL measures
    logger.info("Calculating ALL performance measures...")
    
    # Core DFM measures
    logger.info("Calculating performance_score")
    perf['performance_score'] = perf.apply(lambda row:
        calculate_performance_score_enhanced(
            row['result_value'], 
            row['event_name'], 
            row['measurement_unit'],
            row['gender'] 
        ), axis=1)

    logger.info("Calculating altitude_adjusted_result")
    perf['altitude_adjusted_result'] = perf.apply(lambda row:
        calculate_altitude_adjustment(row['result_value'], row['altitude'], row['event_name'], row['measurement_unit']), axis=1)

    # Environmental impact measures
    logger.info("Calculating temperature_impact_factor")
    perf['temperature_impact_factor'] = perf.apply(lambda row:
        calculate_temperature_impact_factor(row['temperature'], row['event_name']), axis=1)


    # Step 10: Add performance context flags (simplified without competition logic)
    logger.info("Adding performance context flags...")
    
    # Simplified championship detection
    perf['has_wind_data'] = pd.notna(perf['wind_reading'])
    perf['load_batch_id'] = 1


    # Remove performances with missing data directly
    perf = perf.dropna(subset=['date_key'])
    perf = perf.dropna(subset=['venue_key'])

    # # Step 11: Handle missing dimension keys with defaults
    # perf['date_key'] = perf['date_key'].fillna(1)
    # perf['venue_key'] = perf['venue_key'].fillna(1)
    # perf['weather_key'] = perf['weather_key'].fillna(1)

    # Rename to match schema
    perf['rank_position'] = perf['position_finish']


    # Step 12: Select final columns for fact table
    fact_cols = [
        # 5 ESSENTIAL FOREIGN KEYS
        'athlete_key',      # WHO
        'event_key',        # WHAT
        'venue_key',        # WHERE
        'date_key',         # WHEN
        'weather_key',      # CONDITIONS
        
        # Athlete Context
        'gender',

        # Primary Results
        'result_value', 'rank_position', 'wind_reading',
        
        # Standardized Measures
        'performance_score',
        
        # Environmental Impact Measures
        'altitude_adjusted_result', 'temperature_impact_factor',
        
        # Additional Quality Measures
        'has_wind_data',
        
        # Data Quality
        'data_quality_score', 'data_source', 'load_batch_id'
    ]

    final_df = perf[fact_cols].copy()


    # Step 13: Simplified success summary
    logger.info("SIMPLIFIED FACT TABLE SUMMARY:")
    logger.info(f"Total performances: {len(final_df):,}")
    logger.info(f"Unique athletes: {final_df['athlete_key'].nunique():,}")
    logger.info(f"Unique events: {final_df['event_key'].nunique():,}")
    logger.info(f"Unique venues: {final_df['venue_key'].nunique():,}")
    logger.info(f"Unique dates: {final_df['date_key'].nunique():,}")
    logger.info(f"Unique weather conditions: {final_df['weather_key'].nunique():,}")
    
    logger.info("ALL CALCULATION FUNCTIONS USED:")
    logger.info(f"performance_score: avg = {final_df['performance_score'].mean():.1f}")
    logger.info(f"altitude_adjusted_result: avg = {final_df['altitude_adjusted_result'].mean():.3f}")
    logger.info(f"temperature_impact_factor: avg = {final_df['temperature_impact_factor'].mean():.3f}")


    # Step 14: Load to database
    logger.info(f"Loading {len(final_df)} records to dwh.fact_performance...")
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dwh.fact_performance RESTART IDENTITY"))
        conn.commit()
        
        final_df.to_sql('fact_performance', conn, schema='dwh', if_exists='append', index=False)
        conn.commit()
        


def main():
    try:
        logger.info("Starting fact table loading...")
        engine = create_db_connection()
        load_fact_table(engine)

        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_performance")).scalar()
            logger.info(f"Total fact records: {count}")

    except Exception as e:
        logger.error(f"Fact loading failed: {e}")
        raise

if __name__ == "__main__":
    main()
