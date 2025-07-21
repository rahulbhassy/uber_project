# generate_data.py
import random
from faker import Faker
from datetime import datetime
import os
from pyspark.sql import DataFrame

# Import all required classes from the previous code
from DGFunctions import (
    GetData, DataGenerator, Combiner, TripGenerator, DataSaver
)


def generate_new_data(fake, max_customer_id, max_driver_id, num_customers=35, num_drivers=12, num_vehicles=7):
    """Generate new customer, driver, and vehicle data"""
    generator = DataGenerator(fake, max_customer_id, max_driver_id)
    return {
        'customers': generator.generate_new_customers(num_customers),
        'drivers': generator.generate_new_drivers(num_drivers),
        'vehicles': generator.generate_new_vehicles(num_vehicles)
    }


def main():
    print("Starting data generation process...")
    fake = Faker()
    Faker.seed(42)  # For reproducible results

    # 1. Load existing data
    print("\n1. Loading existing data...")
    customer_getter = GetData("customerdetails")
    existing_customers = customer_getter.read_existing_data()

    driver_getter = GetData("driverdetails")
    existing_drivers = driver_getter.read_existing_data()

    vehicle_getter = GetData("vehicledetails")
    existing_vehicles = vehicle_getter.read_existing_data()

    # 2. Get max IDs for generation
    print("\n2. Calculating max IDs...")
    max_customer_id = customer_getter.get_max_ids(existing_customers) or 0
    max_driver_id = driver_getter.get_max_ids(existing_drivers) or 0
    print(f"Max Customer ID: {max_customer_id}, Max Driver ID: {max_driver_id}")

    # 3. Generate new entities
    print("\n3. Generating new entities...")
    new_data = generate_new_data(
        fake=fake,
        max_customer_id=max_customer_id,
        max_driver_id=max_driver_id,
        num_customers=35,
        num_drivers=12,
        num_vehicles=7
    )

    # 4. Combine with existing data
    print("\n4. Combining with existing data...")
    combiner = Combiner(
        existing_customers_df=existing_customers,
        existing_drivers_df=existing_drivers,
        existing_vehicles_df=existing_vehicles
    ).create_combined_pools(
        new_customers=new_data['customers'],
        new_drivers=new_data['drivers'],
        new_vehicles=new_data['vehicles']
    )

    # 5. Generate trip details
    print("\n5. Generating trip details...")
    # Get existing trip IDs
    trip_getter = GetData("uberfares")
    new_trip_ids = trip_getter._get_trip_ids()

    # Generate new trip IDs (1000 trips)

    # Generate trip details
    trip_generator = TripGenerator(combiner, fake)
    trip_details = trip_generator.generate_trip_details(new_trip_ids)

    # 6. Save all data
    print("\n6. Saving data to CSV files...")
    data_saver = DataSaver()
    data_saver.save_all(
        new_customers=new_data['customers'],
        new_drivers=new_data['drivers'],
        new_vehicles=new_data['vehicles'],
        trip_details=trip_details
    )

    # 7. Generate summary report
    print("\n7. Generating summary report...")
    data_saver.generate_summary_report()

    print("\nData generation completed successfully!")


if __name__ == "__main__":
    main()