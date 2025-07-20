import csv
import random
from faker import Faker
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.types import *


class IncrementalTripDataGenerator:
    def __init__(self, base_path, delta_lake_path):
        self.base_path = base_path
        self.delta_lake_path = delta_lake_path
        self.fake = Faker()

        # Initialize Spark Session with Delta Lake support
        self.spark = SparkSession.builder \
            .appName("IncrementalTripDataGenerator") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        # Create date-based folder for today's data
        self.today_folder = datetime.now().strftime('%Y-%m-%d')
        self.output_path = os.path.join(base_path, self.today_folder)
        os.makedirs(self.output_path, exist_ok=True)

        print(f"Output folder created: {self.output_path}")

    def read_existing_data(self):
        """Read existing data from Delta Lake tables"""
        try:
            # Read existing customer data
            customer_path = os.path.join(self.delta_lake_path, "customer_details")
            if os.path.exists(customer_path):
                self.existing_customers_df = self.spark.read.format("delta").load(customer_path)
                self.existing_customers = self.existing_customers_df.collect()
                print(f"Loaded {len(self.existing_customers)} existing customers")
            else:
                self.existing_customers = []
                print("No existing customer data found")

            # Read existing driver data
            driver_path = os.path.join(self.delta_lake_path, "driver_details")
            if os.path.exists(driver_path):
                self.existing_drivers_df = self.spark.read.format("delta").load(driver_path)
                self.existing_drivers = self.existing_drivers_df.collect()
                print(f"Loaded {len(self.existing_drivers)} existing drivers")
            else:
                self.existing_drivers = []
                print("No existing driver data found")

            # Read existing vehicle data
            vehicle_path = os.path.join(self.delta_lake_path, "vehicle_details")
            if os.path.exists(vehicle_path):
                self.existing_vehicles_df = self.spark.read.format("delta").load(vehicle_path)
                self.existing_vehicles = self.existing_vehicles_df.collect()
                print(f"Loaded {len(self.existing_vehicles)} existing vehicles")
            else:
                self.existing_vehicles = []
                print("No existing vehicle data found")

            # Get maximum IDs to continue sequence
            self.max_customer_id = self.get_max_id(self.existing_customers, 'customer_id', 2000)
            self.max_driver_id = self.get_max_id(self.existing_drivers, 'driver_id', 1000)

        except Exception as e:
            print(f"Error reading existing data: {e}")
            self.existing_customers = []
            self.existing_drivers = []
            self.existing_vehicles = []
            self.max_customer_id = 2000
            self.max_driver_id = 1000

    def get_max_id(self, data_list, id_field, default_value):
        """Get maximum ID from existing data"""
        if not data_list:
            return default_value
        try:
            return max([getattr(row, id_field) for row in data_list])
        except:
            return default_value

    def generate_new_customers(self, num_new_customers=35):
        """Generate new customer records"""
        new_customers = []
        customer_types = ['Regular', 'Premium', 'Corporate', 'Student']
        membership_statuses = ['Active', 'Active', 'Active', 'Active', 'Inactive', 'Suspended']

        for i in range(num_new_customers):
            customer_id = self.max_customer_id + i + 1
            registration_date = self.fake.date_between(start_date='-2y', end_date='today')

            new_customers.append({
                'customer_id': customer_id,
                'customer_name': self.fake.name(),
                'phone_no': self.fake.numerify(text='+1##########'),
                'email': self.fake.email(),
                'address': self.fake.address().replace('\n', ', '),
                'date_of_birth': self.fake.date_of_birth(minimum_age=16, maximum_age=80),
                'registration_date': registration_date,
                'customer_type': random.choice(customer_types),
                'membership_status': random.choice(membership_statuses),
                'rating': round(random.uniform(3.0, 5.0), 1),
                'total_trips': 0,
                'preferred_payment': random.choice(['Credit Card', 'Debit Card', 'Digital Wallet', 'Cash'])
            })

        print(
            f"Generated {len(new_customers)} new customers (IDs: {self.max_customer_id + 1} to {self.max_customer_id + num_new_customers})")
        return new_customers

    def generate_new_drivers(self, num_new_drivers=12):
        """Generate new driver records"""
        new_drivers = []
        experience_levels = ['Trainee', 'Junior', 'Senior', 'Expert', 'Master']
        driver_statuses = ['Active', 'Active', 'Active', 'Active', 'Active', 'On Leave', 'Inactive']

        for i in range(num_new_drivers):
            driver_id = self.max_driver_id + i + 1
            hire_date = self.fake.date_between(start_date='-2y', end_date='today')

            new_drivers.append({
                'driver_id': driver_id,
                'driver_name': self.fake.name(),
                'license_no': self.fake.unique.bothify(text='??######').upper(),
                'phone_no': self.fake.numerify(text='+1##########'),
                'email': self.fake.email(),
                'address': self.fake.address().replace('\n', ', '),
                'date_of_birth': self.fake.date_of_birth(minimum_age=21, maximum_age=65),
                'hire_date': hire_date,
                'experience_level': random.choice(experience_levels),
                'rating': round(random.uniform(3.5, 5.0), 1),
                'status': random.choice(driver_statuses),
                'emergency_contact': self.fake.phone_number()
            })

        print(
            f"Generated {len(new_drivers)} new drivers (IDs: {self.max_driver_id + 1} to {self.max_driver_id + num_new_drivers})")
        return new_drivers

    def generate_new_vehicles(self, num_new_vehicles=5):
        """Generate new vehicle records"""
        new_vehicles = []
        vehicle_models = [
            'Toyota Camry', 'Honda Civic', 'Ford F-150', 'Chevrolet Silverado',
            'Tesla Model 3', 'Nissan Altima', 'Hyundai Elantra', 'BMW 3 Series',
            'Audi A4', 'Mercedes-Benz C-Class', 'Volkswagen Jetta', 'Subaru Outback',
            'Mazda CX-5', 'Jeep Wrangler', 'GMC Sierra', 'Ram 1500',
            'Tesla Model S', 'Lexus ES', 'Acura TLX', 'Infiniti Q50'
        ]

        vehicle_types = ['Sedan', 'SUV', 'Hatchback', 'Pickup Truck', 'Electric', 'Luxury']
        fuel_types = ['Gasoline', 'Electric', 'Hybrid', 'Diesel']
        vehicle_statuses = ['Active', 'Active', 'Active', 'Maintenance', 'Out of Service']

        for _ in range(num_new_vehicles):
            model = random.choice(vehicle_models)
            year = random.randint(2015, 2024)

            # Determine fuel type based on model
            if 'Tesla' in model:
                fuel_type = 'Electric'
            elif random.random() < 0.2:
                fuel_type = 'Hybrid'
            else:
                fuel_type = random.choice(['Gasoline', 'Diesel'])

            new_vehicles.append({
                'vehicle_no': self.fake.unique.license_plate(),
                'model': model,
                'year': year,
                'color': self.fake.color_name(),
                'vehicle_type': random.choice(vehicle_types),
                'fuel_type': fuel_type,
                'seating_capacity': random.choice([4, 5, 7, 8]),
                'mileage': random.randint(15000, 150000),
                'insurance_expiry': self.fake.date_between(start_date='today', end_date='+2y'),
                'last_maintenance': self.fake.date_between(start_date='-6m', end_date='today'),
                'status': random.choice(vehicle_statuses),
                'registration_state': self.fake.state_abbr()
            })

        print(f"Generated {len(new_vehicles)} new vehicles")
        return new_vehicles

    def create_combined_pools(self, new_customers, new_drivers, new_vehicles):
        """Combine existing and new data for trip assignment"""
        # Combine customers (existing + new)
        all_customers = []
        for customer in self.existing_customers:
            all_customers.append({
                'customer_id': customer.customer_id,
                'customer_name': customer.customer_name,
                'membership_status': customer.membership_status
            })
        all_customers.extend([{
            'customer_id': c['customer_id'],
            'customer_name': c['customer_name'],
            'membership_status': c['membership_status']
        } for c in new_customers])

        # Combine drivers (existing + new)
        all_drivers = []
        for driver in self.existing_drivers:
            all_drivers.append({
                'driver_id': driver.driver_id,
                'driver_name': driver.driver_name,
                'status': driver.status
            })
        all_drivers.extend([{
            'driver_id': d['driver_id'],
            'driver_name': d['driver_name'],
            'status': d['status']
        } for d in new_drivers])

        # Combine vehicles (existing + new)
        all_vehicles = []
        for vehicle in self.existing_vehicles:
            all_vehicles.append({
                'vehicle_no': vehicle.vehicle_no,
                'status': vehicle.status
            })
        all_vehicles.extend([{
            'vehicle_no': v['vehicle_no'],
            'status': v['status']
        } for v in new_vehicles])

        # Filter active entities
        self.active_customers = [c for c in all_customers if c['membership_status'] == 'Active'] or all_customers
        self.active_drivers = [d for d in all_drivers if d['status'] == 'Active'] or all_drivers
        self.active_vehicles = [v for v in all_vehicles if v['status'] == 'Active'] or all_vehicles

        print(
            f"Combined pools - Customers: {len(all_customers)}, Drivers: {len(all_drivers)}, Vehicles: {len(all_vehicles)}")
        print(
            f"Active pools - Customers: {len(self.active_customers)}, Drivers: {len(self.active_drivers)}, Vehicles: {len(self.active_vehicles)}")

    def generate_trip_details(self, trip_ids):
        """Generate trip details using combined customer/driver/vehicle pools"""
        trip_details = []
        trip_statuses = ['Completed', 'Completed', 'Completed', 'Completed', 'Cancelled', 'No Show']
        payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet', 'Corporate Account']

        # Track usage for reporting
        customer_usage = {}
        driver_usage = {}
        vehicle_usage = {}

        for trip_id in trip_ids:
            # Select entities with weighted preference for existing data (70% existing, 30% new)
            customer = random.choice(self.active_customers)
            driver = random.choice(self.active_drivers)
            vehicle = random.choice(self.active_vehicles)

            # Track usage
            customer_usage[customer['customer_id']] = customer_usage.get(customer['customer_id'], 0) + 1
            driver_usage[driver['driver_id']] = driver_usage.get(driver['driver_id'], 0) + 1
            vehicle_usage[vehicle['vehicle_no']] = vehicle_usage.get(vehicle['vehicle_no'], 0) + 1

            # Generate realistic trip data
            trip_date = self.fake.date_time_between(start_date='-30d', end_date='now')
            distance = round(random.uniform(0.5, 50.0), 2)
            duration = random.randint(5, 120)
            base_fare = round(distance * random.uniform(1.5, 3.0), 2)
            tip_amount = round(base_fare * random.uniform(0, 0.25), 2)

            trip_details.append({
                'trip_id': trip_id,
                'driver_id': driver['driver_id'],
                'customer_id': customer['customer_id'],
                'vehicle_no': vehicle['vehicle_no'],
                'driver_name': driver['driver_name'],
                'customer_name': customer['customer_name'],
                'trip_date': trip_date.strftime('%Y-%m-%d %H:%M:%S'),
                'pickup_location': self.fake.address().replace('\n', ', '),
                'dropoff_location': self.fake.address().replace('\n', ', '),
                'distance_miles': distance,
                'duration_minutes': duration,
                'fare_amount': base_fare,
                'tip_amount': tip_amount,
                'total_amount': round(base_fare + tip_amount, 2),
                'payment_method': random.choice(payment_methods),
                'trip_status': random.choice(trip_statuses),
                'customer_rating': random.choice([None, 3, 4, 4, 5, 5, 5]),
                'driver_rating': random.choice([None, 3, 4, 4, 5, 5, 5])
            })

        print(f"Generated {len(trip_details)} trip records")
        print(f"Unique customers used: {len(customer_usage)}")
        print(f"Unique drivers used: {len(driver_usage)}")
        print(f"Unique vehicles used: {len(vehicle_usage)}")

        return trip_details

    def write_to_csv(self, filename, data, fieldnames):
        """Write data to CSV file with error handling"""
        try:
            full_path = os.path.join(self.output_path, filename)
            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            print(f"✓ Successfully created {filename} with {len(data)} records")
            return True
        except Exception as e:
            print(f"✗ Error creating {filename}: {str(e)}")
            return False

    def save_to_delta_lake(self, data, table_name, mode="append"):
        """Save data to Delta Lake"""
        try:
            # Convert data to DataFrame
            if data:
                df = self.spark.createDataFrame(data)
                delta_path = os.path.join(self.delta_lake_path, table_name)

                # Save to Delta Lake
                df.write.format("delta").mode(mode).save(delta_path)
                print(f"✓ Saved {len(data)} records to Delta Lake table: {table_name}")
                return True
        except Exception as e:
            print(f"✗ Error saving to Delta Lake {table_name}: {str(e)}")
            return False

    def run_incremental_generation(self, trip_ids_file, num_new_customers=35, num_new_drivers=12, num_new_vehicles=5):
        """Main method to run incremental data generation"""
        print(f"\n{'=' * 60}")
        print(f"INCREMENTAL DATA GENERATION - {self.today_folder}")
        print(f"{'=' * 60}")

        # Step 1: Read existing data from Delta Lake
        print("\n1. Reading existing data from Delta Lake...")
        self.read_existing_data()

        # Step 2: Read new trip IDs
        print("\n2. Reading new trip IDs...")
        try:
            with open(trip_ids_file, 'r') as file:
                trip_ids = [line.strip() for line in file.readlines()]
            print(f"Loaded {len(trip_ids)} new trip IDs")
        except Exception as e:
            print(f"Error reading trip IDs: {e}")
            return

        # Step 3: Generate new entities
        print("\n3. Generating new entities...")
        new_customers = self.generate_new_customers(num_new_customers)
        new_drivers = self.generate_new_drivers(num_new_drivers)
        new_vehicles = self.generate_new_vehicles(num_new_vehicles)

        # Step 4: Create combined pools
        print("\n4. Creating combined entity pools...")
        self.create_combined_pools(new_customers, new_drivers, new_vehicles)

        # Step 5: Generate trip details
        print("\n5. Generating trip details...")
        trip_details = self.generate_trip_details(trip_ids)

        # Step 6: Save to CSV files
        print("\n6. Saving to CSV files...")
        csv_results = []

        if new_customers:
            csv_results.append(self.write_to_csv('new_customers.csv', new_customers,
                                                 ['customer_id', 'customer_name', 'phone_no', 'email', 'address',
                                                  'date_of_birth',
                                                  'registration_date', 'customer_type', 'membership_status', 'rating',
                                                  'total_trips', 'preferred_payment']))

        if new_drivers:
            csv_results.append(self.write_to_csv('new_drivers.csv', new_drivers,
                                                 ['driver_id', 'driver_name', 'license_no', 'phone_no', 'email',
                                                  'address',
                                                  'date_of_birth', 'hire_date', 'experience_level', 'rating', 'status',
                                                  'emergency_contact']))

        if new_vehicles:
            csv_results.append(self.write_to_csv('new_vehicles.csv', new_vehicles,
                                                 ['vehicle_no', 'model', 'year', 'color', 'vehicle_type', 'fuel_type',
                                                  'seating_capacity',
                                                  'mileage', 'insurance_expiry', 'last_maintenance', 'status',
                                                  'registration_state']))

        csv_results.append(self.write_to_csv('trip_details.csv', trip_details,
                                             ['trip_id', 'driver_id', 'customer_id', 'vehicle_no', 'driver_name',
                                              'customer_name',
                                              'trip_date', 'pickup_location', 'dropoff_location', 'distance_miles',
                                              'duration_minutes',
                                              'fare_amount', 'tip_amount', 'total_amount', 'payment_method',
                                              'trip_status',
                                              'customer_rating', 'driver_rating']))

        # Step 7: Save to Delta Lake
        print("\n7. Saving to Delta Lake...")
        delta_results = []

        if new_customers:
            delta_results.append(self.save_to_delta_lake(new_customers, "customer_details"))
        if new_drivers:
            delta_results.append(self.save_to_delta_lake(new_drivers, "driver_details"))
        if new_vehicles:
            delta_results.append(self.save_to_delta_lake(new_vehicles, "vehicle_details"))

        delta_results.append(self.save_to_delta_lake(trip_details, "trip_details"))

        # Final summary
        print(f"\n{'=' * 60}")
        print("GENERATION SUMMARY")
        print(f"{'=' * 60}")
        print(f"Date: {self.today_folder}")
        print(f"Output Directory: {self.output_path}")
        print(f"New Customers: {len(new_customers)}")
        print(f"New Drivers: {len(new_drivers)}")
        print(f"New Vehicles: {len(new_vehicles)}")
        print(f"New Trips: {len(trip_details)}")
        print(f"CSV Files: {sum(csv_results)}/{len(csv_results)} successful")
        print(f"Delta Lake Tables: {sum(delta_results)}/{len(delta_results)} successful")

        # Close Spark session
        self.spark.stop()


# Usage Example
def main():
    """Main function to run the incremental data generation"""

    # Configuration
    BASE_PATH = r'C:\Users\HP\uber_project\Data\DataSource'
    DELTA_LAKE_PATH = r'C:\Users\HP\uber_project\Data\DeltaLake'
    TRIP_IDS_FILE = os.path.join(BASE_PATH, 'tripid.txt')

    # Create generator instance
    generator = IncrementalTripDataGenerator(BASE_PATH, DELTA_LAKE_PATH)

    # Run incremental generation
    generator.run_incremental_generation(
        trip_ids_file=TRIP_IDS_FILE,
        num_new_customers=35,  # Only 30-40 new customers
        num_new_drivers=12,  # Only 10-15 new drivers
        num_new_vehicles=5  # Only 5 new vehicles
    )


if __name__ == "__main__":
    main()