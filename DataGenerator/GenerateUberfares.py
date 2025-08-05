"""
Optimized Uber NYC Data Generator
Generates 2,000,000 Uber trip records with dynamic pricing and embedded fraud patterns
Columns: key, dropoff_datetime, fare_amount, pickup_datetime, pickup_longitude,
          pickup_latitude, dropoff_longitude, dropoff_latitude, passenger_count
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import math

# Configuration for fare calculation and fraud patterns
FARE_CONFIG = {
    'base_fare': 2.5,
    'per_km_rates': {
        'petrol': 1.75,
        'diesel': 1.65,
        'hybrid': 1.55,
        'ev': 1.45
    },
    'per_minute_rate': 0.35,
    'fuel_surcharge': {
        'petrol': 0.25,
        'diesel': 0.20,
        'hybrid': 0.15,
        'ev': 0.05
    },
    'fraud_params': {
        'fraud_rate': 0.015,  # 1.5% of trips have fraud patterns
        'distance_multiplier_range': (1.15, 1.8),
        'time_multiplier_range': (1.1, 1.6),
        'short_trip_threshold': 1.5,  # km
        'long_trip_threshold': 30,  # km
        'fraud_short_trip_rate': 0.25,
        'fraud_long_trip_rate': 0.15
    }
}


def generate_uber_nyc_data(total_rows=2000000, output_filename="uber_nyc_2million_rows.csv"):
    """
    Generate realistic Uber NYC trip data with dynamic pricing

    Parameters:
    total_rows (int): Number of rows to generate (default: 2,000,000)
    output_filename (str): Output CSV filename
    """

    print(f"Generating {total_rows:,} Uber NYC trip records...")

    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)

    # Generate 150+ NYC boroughs and municipalities
    nyc_areas = generate_nyc_areas()
    areas = list(nyc_areas.keys())

    # Area distribution weights (most trips in main boroughs)
    area_weights = calculate_area_weights(areas)

    print(f"Using {len(areas)} boroughs/municipalities with weighted distribution")

    # Generate data in batches to manage memory
    batch_size = 100000  # 100k rows per batch
    num_batches = total_rows // batch_size

    print(f"Processing in {num_batches} batches of {batch_size:,} rows each...")

    # Initialize CSV file
    first_batch = True

    for batch_num in range(num_batches):
        print(f"Processing batch {batch_num + 1}/{num_batches}...")

        # Generate batch data
        batch_data = generate_batch(batch_size, nyc_areas, areas, area_weights)

        # Convert to DataFrame
        batch_df = pd.DataFrame(batch_data)

        # Save batch to CSV
        if first_batch:
            batch_df.to_csv(output_filename, mode='w', index=False)
            first_batch = False
        else:
            batch_df.to_csv(output_filename, mode='a', index=False, header=False)

        print(f"  Saved batch {batch_num + 1}. Total progress: {(batch_num + 1) * batch_size:,}/{total_rows:,} rows")

    # Verify the final file
    print(f"\nDataset generation complete!")
    print(f"Output file: {output_filename}")

    # Get file statistics
    file_size_mb = os.path.getsize(output_filename) / (1024 * 1024)
    print(f"File size: {file_size_mb:.1f} MB")

    # Show sample data
    sample_df = pd.read_csv(output_filename, nrows=5)
    print(f"\nFirst 5 rows of generated dataset:")
    print(sample_df)

    return output_filename


def generate_nyc_areas():
    """Generate 150+ NYC boroughs and municipalities with realistic coordinates"""
    # Core NYC boroughs with precise bounds
    areas = {
        # Manhattan neighborhoods
        "Financial District": {"lat_min": 40.703, "lat_max": 40.712, "lon_min": -74.018, "lon_max": -74.008},
        "Midtown": {"lat_min": 40.748, "lat_max": 40.761, "lon_min": -73.985, "lon_max": -73.968},
        "Upper East Side": {"lat_min": 40.768, "lat_max": 40.787, "lon_min": -73.959, "lon_max": -73.947},
        "Upper West Side": {"lat_min": 40.775, "lat_max": 40.799, "lon_min": -73.982, "lon_max": -73.962},
        "Harlem": {"lat_min": 40.799, "lat_max": 40.816, "lon_min": -73.952, "lon_max": -73.932},
        "East Village": {"lat_min": 40.720, "lat_max": 40.732, "lon_min": -73.985, "lon_max": -73.975},

        # Brooklyn neighborhoods
        "Williamsburg": {"lat_min": 40.705, "lat_max": 40.718, "lon_min": -73.965, "lon_max": -73.945},
        "DUMBO": {"lat_min": 40.699, "lat_max": 40.704, "lon_min": -73.998, "lon_max": -73.987},
        "Park Slope": {"lat_min": 40.668, "lat_max": 40.680, "lon_min": -73.990, "lon_max": -73.975},
        "Coney Island": {"lat_min": 40.570, "lat_max": 40.580, "lon_min": -74.000, "lon_max": -73.975},
        "Brooklyn Heights": {"lat_min": 40.694, "lat_max": 40.702, "lon_min": -74.005, "lon_max": -73.991},

        # Queens neighborhoods
        "Astoria": {"lat_min": 40.756, "lat_max": 40.776, "lon_min": -73.928, "lon_max": -73.905},
        "Long Island City": {"lat_min": 40.740, "lat_max": 40.755, "lon_min": -73.960, "lon_max": -73.935},
        "Flushing": {"lat_min": 40.755, "lat_max": 40.770, "lon_min": -73.835, "lon_max": -73.810},
        "Jackson Heights": {"lat_min": 40.745, "lat_max": 40.755, "lon_min": -73.885, "lon_max": -73.870},

        # Bronx neighborhoods
        "Fordham": {"lat_min": 40.855, "lat_max": 40.865, "lon_min": -73.900, "lon_max": -73.885},
        "Pelham Bay": {"lat_min": 40.845, "lat_max": 40.860, "lon_min": -73.825, "lon_max": -73.800},

        # Staten Island neighborhoods
        "St. George": {"lat_min": 40.635, "lat_max": 40.645, "lon_min": -74.080, "lon_max": -74.065},
        "Tottenville": {"lat_min": 40.505, "lat_max": 40.515, "lon_min": -74.255, "lon_max": -74.240},

        # Major airports
        "JFK Airport": {"lat_min": 40.635, "lat_max": 40.655, "lon_min": -73.790, "lon_max": -73.760},
        "LaGuardia Airport": {"lat_min": 40.770, "lat_max": 40.780, "lon_min": -73.885, "lon_max": -73.860},
        "Newark Airport": {"lat_min": 40.680, "lat_max": 40.700, "lon_min": -74.190, "lon_max": -74.160},

        # New Jersey cities
        "Jersey City": {"lat_min": 40.710, "lat_max": 40.730, "lon_min": -74.055, "lon_max": -74.030},
        "Hoboken": {"lat_min": 40.735, "lat_max": 40.745, "lon_min": -74.035, "lon_max": -74.020},
        "Newark": {"lat_min": 40.725, "lat_max": 40.745, "lon_min": -74.190, "lon_max": -74.140},
        "Elizabeth": {"lat_min": 40.655, "lat_max": 40.675, "lon_min": -74.220, "lon_max": -74.190},

        # Long Island areas
        "Nassau County": {"lat_min": 40.720, "lat_max": 40.800, "lon_min": -73.650, "lon_max": -73.500},
        "Suffolk County": {"lat_min": 40.800, "lat_max": 40.950, "lon_min": -73.200, "lon_max": -72.700},

        # Westchester and Connecticut
        "Yonkers": {"lat_min": 40.925, "lat_max": 40.945, "lon_min": -73.900, "lon_max": -73.880},
        "White Plains": {"lat_min": 41.025, "lat_max": 41.040, "lon_min": -73.780, "lon_max": -73.760},
        "Stamford": {"lat_min": 41.035, "lat_max": 41.060, "lon_min": -73.570, "lon_max": -73.520},
    }

    # Add more areas to reach 150+
    for i in range(1, 126):  # 125 additional areas
        # Create semi-realistic areas around NYC
        lat = 40.5 + random.random() * 1.5  # 40.5-42.0
        lon = -74.5 + random.random() * 3.0  # -74.5 to -71.5

        # Create bounding box around the point
        lat_min = lat - random.uniform(0.005, 0.02)
        lat_max = lat + random.uniform(0.005, 0.02)
        lon_min = lon - random.uniform(0.005, 0.03)
        lon_max = lon + random.uniform(0.005, 0.03)

        areas[f"Area_{i:03}"] = {
            "lat_min": lat_min,
            "lat_max": lat_max,
            "lon_min": lon_min,
            "lon_max": lon_max
        }

    return areas


def calculate_area_weights(areas):
    """Calculate weights favoring core NYC boroughs"""
    weights = {}
    core_areas = ["Financial District", "Midtown", "Upper East Side", "Upper West Side",
                  "Williamsburg", "DUMBO", "Park Slope", "Astoria", "Long Island City",
                  "Fordham", "St. George", "JFK Airport", "LaGuardia Airport", "Jersey City"]

    for area in areas:
        if area in core_areas:
            weights[area] = 15  # Highest weight for core areas
        elif "Airport" in area:
            weights[area] = 8  # Medium weight for airports
        elif area.startswith("Area_"):
            weights[area] = 1  # Lowest weight for peripheral areas
        else:
            weights[area] = 5  # Medium weight for other named areas

    # Normalize weights to probabilities
    total = sum(weights.values())
    return {area: weight / total for area, weight in weights.items()}


def generate_batch(batch_size, nyc_areas, areas, area_weights):
    """Generate a batch of trip data with only required columns"""
    # Generate unique keys
    keys = np.random.randint(10000000, 99999999, size=batch_size)

    # Select pickup and dropoff areas
    pickup_areas = np.random.choice(
        list(area_weights.keys()),
        size=batch_size,
        p=list(area_weights.values())
    )

    # 70% chance dropoff is same area, 30% different area
    same_area_mask = np.random.random(batch_size) < 0.7
    dropoff_areas = np.where(
        same_area_mask,
        pickup_areas,
        np.random.choice(list(area_weights.keys()), size=batch_size, p=list(area_weights.values()))
    )

    # Generate coordinates
    pickup_lats, pickup_lons = generate_coordinates_batch(pickup_areas, nyc_areas)
    dropoff_lats, dropoff_lons = generate_coordinates_batch(dropoff_areas, nyc_areas)

    # Generate passenger counts (realistic distribution)
    passenger_counts = np.random.choice([1, 2, 3, 4, 5, 6], size=batch_size,
                                        p=[0.6, 0.25, 0.08, 0.04, 0.02, 0.01])

    # Generate datetimes (2009-2015 range)
    pickup_datetimes, dropoff_datetimes, durations = generate_datetimes_batch(batch_size)

    # Calculate realistic distances
    distances = calculate_haversine_distances(pickup_lats, pickup_lons, dropoff_lats, dropoff_lons)

    # Calculate fares with dynamic pricing and embedded fraud patterns
    fare_amounts = calculate_fares_with_embedded_fraud(
        distances,
        durations,
        pickup_datetimes
    )

    # Return batch data with only required columns
    return {
        'key': keys,
        'pickup_datetime': pickup_datetimes,
        'dropoff_datetime': dropoff_datetimes,
        'pickup_longitude': np.round(pickup_lons, 6),
        'pickup_latitude': np.round(pickup_lats, 6),
        'dropoff_longitude': np.round(dropoff_lons, 6),
        'dropoff_latitude': np.round(dropoff_lats, 6),
        'passenger_count': passenger_counts,
        'fare_amount': np.round(fare_amounts, 2)
    }


def generate_coordinates_batch(areas_array, nyc_areas):
    """Generate coordinates for a batch based on area assignments"""
    lats = np.zeros(len(areas_array))
    lons = np.zeros(len(areas_array))

    for i, area in enumerate(areas_array):
        coords = nyc_areas[area]
        lats[i] = random.uniform(coords['lat_min'], coords['lat_max'])
        lons[i] = random.uniform(coords['lon_min'], coords['lon_max'])

    return lats, lons


def generate_datetimes_batch(batch_size):
    """Generate pickup and dropoff datetimes with durations"""
    # Generate pickup times (2009-2015)
    start_ts = int(datetime(2009, 1, 1).timestamp())
    end_ts = int(datetime(2015, 12, 31).timestamp())

    pickup_timestamps = np.random.randint(start_ts, end_ts, size=batch_size)
    pickup_datetimes = []
    dropoff_datetimes = []
    durations = []

    for ts in pickup_timestamps:
        pickup_dt = datetime.fromtimestamp(ts)

        # Base duration with traffic patterns
        hour = pickup_dt.hour
        if 7 <= hour < 10 or 16 <= hour < 19:  # Rush hours
            base_duration = random.randint(15, 90)
        else:
            base_duration = random.randint(5, 60)

        # Add random variation
        trip_duration = max(3, base_duration + random.randint(-10, 20))

        dropoff_dt = pickup_dt + timedelta(minutes=trip_duration)

        pickup_datetimes.append(pickup_dt.strftime("%Y-%m-%d %H:%M:%S UTC"))
        dropoff_datetimes.append(dropoff_dt.strftime("%Y-%m-%d %H:%M:%S UTC"))
        durations.append(trip_duration)

    return pickup_datetimes, dropoff_datetimes, durations


def calculate_haversine_distances(lat1, lon1, lat2, lon2):
    """Calculate accurate distances using Haversine formula"""
    # Convert to radians
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371  # Earth radius in km
    return c * r


def calculate_fares_with_embedded_fraud(distances, durations, pickup_datetimes):
    """Calculate fare amounts with embedded fraud patterns"""
    base_fare = FARE_CONFIG['base_fare']
    per_minute_rate = FARE_CONFIG['per_minute_rate']

    # Initialize arrays
    fare_amounts = np.zeros(len(distances))

    for i in range(len(distances)):
        # Randomly assign vehicle fuel type for this trip
        fuel_type = random.choices(
            ['petrol', 'diesel', 'hybrid', 'ev'],
            weights=[0.35, 0.35, 0.20, 0.10]
        )[0]

        # Get vehicle-specific rates
        per_km_rate = FARE_CONFIG['per_km_rates'][fuel_type]
        fuel_surcharge = FARE_CONFIG['fuel_surcharge'][fuel_type]

        # Base fare calculation
        fare = base_fare + (distances[i] * per_km_rate) + (durations[i] * per_minute_rate) + fuel_surcharge

        # Apply surge pricing (time-based)
        pickup_dt = datetime.strptime(pickup_datetimes[i], "%Y-%m-%d %H:%M:%S UTC")
        hour = pickup_dt.hour

        # Friday/Saturday night surge
        if pickup_dt.weekday() in [4, 5] and 20 <= hour < 24:
            surge = random.uniform(1.2, 2.0)
            fare *= surge

        # Rush hour surge
        elif (7 <= hour < 10) or (16 <= hour < 19):
            surge = random.uniform(1.1, 1.5)
            fare *= surge

        # Apply fraud patterns (embedded in fare calculation)
        if random.random() < FARE_CONFIG['fraud_params']['fraud_rate']:
            # Short trip fraud - inflate distance
            if distances[i] < FARE_CONFIG['fraud_params']['short_trip_threshold']:
                multiplier = random.uniform(*FARE_CONFIG['fraud_params']['distance_multiplier_range'])
                fraud_dist = distances[i] * multiplier
                fare = base_fare + (fraud_dist * per_km_rate) + (durations[i] * per_minute_rate) + fuel_surcharge

            # Long trip fraud - inflate time
            elif distances[i] > FARE_CONFIG['fraud_params']['long_trip_threshold']:
                multiplier = random.uniform(*FARE_CONFIG['fraud_params']['time_multiplier_range'])
                fraud_time = durations[i] * multiplier
                fare = base_fare + (distances[i] * per_km_rate) + (fraud_time * per_minute_rate) + fuel_surcharge

            # Medium trip fraud - inflate both
            else:
                dist_mult = random.uniform(1.05, 1.3)
                time_mult = random.uniform(1.05, 1.4)
                fraud_dist = distances[i] * dist_mult
                fraud_time = durations[i] * time_mult
                fare = base_fare + (fraud_dist * per_km_rate) + (fraud_time * per_minute_rate) + fuel_surcharge

        # Ensure minimum fare
        fare_amounts[i] = max(4.0, fare)

    return fare_amounts


if __name__ == "__main__":
    # Generate the dataset
    output_file = generate_uber_nyc_data(
        total_rows=2000000,  # 20 lakh rows
        output_filename="uber_nyc_fraud_analysis.csv"
    )

    print(f"\n✓ Successfully generated Uber NYC trip records!")
    print(f"✓ File saved as: {output_file}")
    print(f"✓ Includes 150+ boroughs/municipalities")
    print(f"✓ Dynamic fare calculation based on distance, duration, and vehicle type")
    print(f"✓ Embedded fraud patterns for detection analysis")

    # Final verification
    print(f"\nDataset verification:")
    df_sample = pd.read_csv(output_file, nrows=1000)
    print(f"Coordinate ranges:")
    print(f"  Longitude: {df_sample['pickup_longitude'].min():.4f} to {df_sample['pickup_longitude'].max():.4f}")
    print(f"  Latitude: {df_sample['pickup_latitude'].min():.4f} to {df_sample['pickup_latitude'].max():.4f}")
    print(f"Fare range: ${df_sample['fare_amount'].min():.2f} to ${df_sample['fare_amount'].max():.2f}")
    print(f"Passenger distribution: {dict(df_sample['passenger_count'].value_counts().sort_index())}")