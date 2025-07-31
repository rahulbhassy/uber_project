import csv
import random
from faker import Faker
from datetime import datetime, timedelta
import os
from typing import List, Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import max, col
from Shared.sparkconfig import create_spark_session
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader


_BASE_PATH = 'C:/Users/HP/uber_project/Data/DataSource'

'''    self.today_folder = datetime.now().strftime('%Y-%m-%d')
    self.output_path = os.path.join(self._BASE_PATH, self.today_folder)

    os.makedirs(self.output_path, exist_ok=True)
    print(f"Output folder created: {self.output_path}")'''

class GetData:

    _KEY_COLUMNS = {
        "driverdetails": "driver_id",
        "customerdetails": "customer_id",
        "vehicledetails": "vehicle_no",
    }
    def __init__(self,table):
        self.table = table
        self.spark = create_spark_session()

    def _get_trip_ids(self):
        deltafileio = DataLakeIO(
            process='read',
            table=self.table,
            state='delta',
            layer='raw',
            loadtype='delta'
        )
        deltapath = deltafileio.filepath()
        print(f"Input folder created for getting New Trip Ids: {deltapath}")
        dataloader = DataLoader(
            path=deltapath,
            filetype='parquet'
        )
        rows = dataloader.LoadData(self.spark).select("trip_id").collect()

        # Extract the value from each Row
        trip_ids: List[Any] = [row.trip_id for row in rows]
        print(f"Trip ids loaded with count : {len(trip_ids)}")
        return trip_ids

    def read_existing_data(self):
        currentio = DataLakeIO(
            process='read',
            table=self.table,
            state='current',
            layer='raw',
            loadtype='full'
        )
        dataloader = DataLoader(
            path=currentio.filepath(),
            filetype='delta'
        )

        existingdata = dataloader.LoadData(self.spark)
        print(f"Loaded Existing Data for {self.table} with count: {existingdata.count()}")
        return existingdata

    def get_max_ids(self,df=DataFrame):
        numeric_max_df = df.agg(max(col(self._KEY_COLUMNS[self.table]).cast("long")).alias("max_id"))
        return numeric_max_df.collect()[0]["max_id"]



class DataGenerator:
    def __init__(self, fake, max_customer_id, max_driver_id):
        self.fake = fake
        self.max_customer_id = max_customer_id
        self.max_driver_id = max_driver_id

    def generate_new_customers(self, num_new_customers=50):
        new_customers = []
        customer_types = ['Regular', 'Premium', 'Corporate', 'Student']
        membership_statuses = ['Active', 'Active', 'Active', 'Active', 'Inactive', 'Suspended']

        for i in range(num_new_customers):
            customer_id = self.max_customer_id + i + 1
            registration_date = self.fake.date_between(start_date='-5y', end_date='today')

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

    def generate_new_vehicles(self, num_new_vehicles=7):
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


class Combiner:
    def __init__(self, existing_customers_df, existing_drivers_df, existing_vehicles_df):
        """
        Initialize Combiner with existing data from Delta tables.

        :param existing_customers_df: Spark DataFrame of existing customers
        :param existing_drivers_df: Spark DataFrame of existing drivers
        :param existing_vehicles_df: Spark DataFrame of existing vehicles
        """
        self.existing_customers_df = existing_customers_df
        self.existing_drivers_df = existing_drivers_df
        self.existing_vehicles_df = existing_vehicles_df

        # Will be populated in create_combined_pools()
        self.all_customers = []
        self.all_drivers = []
        self.all_vehicles = []
        self.active_customers = []
        self.active_drivers = []
        self.active_vehicles = []

    def create_combined_pools(self, new_customers, new_drivers, new_vehicles):
        """Combine existing and new data for trip assignment"""
        # Convert existing customer data to dictionaries
        existing_customers = self.existing_customers_df.select(
            "customer_id", "customer_name", "membership_status"
        ).collect()

        # Combine existing + new customers
        self.all_customers = [
                                 {
                                     'customer_id': row.customer_id,
                                     'customer_name': row.customer_name,
                                     'membership_status': row.membership_status
                                 }
                                 for row in existing_customers
                             ] + [
                                 {
                                     'customer_id': c['customer_id'],
                                     'customer_name': c['customer_name'],
                                     'membership_status': c['membership_status']
                                 }
                                 for c in new_customers
                             ]

        # Convert existing driver data to dictionaries
        existing_drivers = self.existing_drivers_df.select(
            "driver_id", "driver_name", "status"
        ).collect()

        # Combine existing + new drivers
        self.all_drivers = [
                               {
                                   'driver_id': row.driver_id,
                                   'driver_name': row.driver_name,
                                   'status': row.status
                               }
                               for row in existing_drivers
                           ] + [
                               {
                                   'driver_id': d['driver_id'],
                                   'driver_name': d['driver_name'],
                                   'status': d['status']
                               }
                               for d in new_drivers
                           ]

        # Convert existing vehicle data to dictionaries
        existing_vehicles = self.existing_vehicles_df.select(
            "vehicle_no", "status"
        ).collect()

        # Combine existing + new vehicles
        self.all_vehicles = [
                                {
                                    'vehicle_no': row.vehicle_no,
                                    'status': row.status
                                }
                                for row in existing_vehicles
                            ] + [
                                {
                                    'vehicle_no': v['vehicle_no'],
                                    'status': v['status']
                                }
                                for v in new_vehicles
                            ]

        # Filter active entities
        self.active_customers = [c for c in self.all_customers if
                                 c['membership_status'] == 'Active'] or self.all_customers
        self.active_drivers = [d for d in self.all_drivers if d['status'] == 'Active'] or self.all_drivers
        self.active_vehicles = [v for v in self.all_vehicles if v['status'] == 'Active'] or self.all_vehicles

        print(
            f"Combined pools - Customers: {len(self.all_customers)}, Drivers: {len(self.all_drivers)}, Vehicles: {len(self.all_vehicles)}")
        print(
            f"Active pools - Customers: {len(self.active_customers)}, Drivers: {len(self.active_drivers)}, Vehicles: {len(self.active_vehicles)}")

        return self


class TripGenerator:
    def __init__(self, combiner, fake):
        """
        Initialize TripGenerator with combined entity pools and Faker instance.

        :param combiner: Combiner instance with active entity pools
        :param fake: Faker instance for generating realistic data
        """
        self.combiner = combiner
        self.fake = fake
        self.customer_usage = {}
        self.driver_usage = {}
        self.vehicle_usage = {}

    def generate_trip_details(self, trip_ids):
        """Generate trip details using combined customer/driver/vehicle pools"""
        trip_details = []
        trip_statuses = ['Completed', 'Completed', 'Completed', 'Completed', 'Cancelled', 'No Show']
        payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet', 'Corporate Account']

        for trip_id in trip_ids:
            # Select random active entities
            customer = random.choice(self.combiner.active_customers)
            driver = random.choice(self.combiner.active_drivers)
            vehicle = random.choice(self.combiner.active_vehicles)

            # Update usage statistics
            self.customer_usage[customer['customer_id']] = self.customer_usage.get(customer['customer_id'], 0) + 1
            self.driver_usage[driver['driver_id']] = self.driver_usage.get(driver['driver_id'], 0) + 1
            self.vehicle_usage[vehicle['vehicle_no']] = self.vehicle_usage.get(vehicle['vehicle_no'], 0) + 1

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
        print(f"Unique customers used: {len(self.customer_usage)}")
        print(f"Unique drivers used: {len(self.driver_usage)}")
        print(f"Unique vehicles used: {len(self.vehicle_usage)}")

        return trip_details


import os
import csv
from datetime import datetime
from tabulate import tabulate


class DataSaver:
    # Base path for all generated data
    _BASE_PATH = 'C:/Users/HP/uber_project/Data/DataSource'

    # Fixed filenames for daily data
    FILENAMES = {
        'customers': 'customerdetails.csv',
        'drivers': 'driverdetails.csv',
        'vehicles': 'vehicledetails.csv',
        'trips': 'tripdetails.csv'
    }

    def __init__(self):
        """
        Initialize DataSaver with dated output directory and fixed filenames
        """
        # Create date-based directory path (format: YYYY-MM-DD)
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.output_path = os.path.join(self._BASE_PATH, self.date_str)
        self.report_data = []

        # Create output directory if it doesn't exist
        os.makedirs(self.output_path, exist_ok=True)
        print(f"Data will be saved to: {self.output_path}")

    def write_to_csv(self, filename, data, fieldnames):
        """Write data to CSV file with error handling"""
        try:
            full_path = os.path.join(self.output_path, filename)

            # Write data to CSV
            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

            # Record success for reporting
            self.report_data.append({
                'file': filename,
                'records': len(data),
                'status': 'Success',
                'message': f"Created with {len(data)} records"
            })
            return True
        except Exception as e:
            # Record failure for reporting
            self.report_data.append({
                'file': filename,
                'records': 0,
                'status': 'Failed',
                'message': str(e)
            })
            return False

    def save_all(self, new_customers, new_drivers, new_vehicles, trip_details):
        """Save all generated data to CSV files with fixed filenames"""
        results = []

        # Define field mappings for each entity type
        field_mappings = {
            'customers': [
                'customer_id', 'customer_name', 'phone_no', 'email', 'address',
                'date_of_birth', 'registration_date', 'customer_type',
                'membership_status', 'rating', 'total_trips', 'preferred_payment'
            ],
            'drivers': [
                'driver_id', 'driver_name', 'license_no', 'phone_no', 'email',
                'address', 'date_of_birth', 'hire_date', 'experience_level',
                'rating', 'status', 'emergency_contact'
            ],
            'vehicles': [
                'vehicle_no', 'model', 'year', 'color', 'vehicle_type',
                'fuel_type', 'seating_capacity', 'mileage', 'insurance_expiry',
                'last_maintenance', 'status', 'registration_state'
            ],
            'trips': [
                'trip_id', 'driver_id', 'customer_id', 'vehicle_no', 'driver_name',
                'customer_name', 'trip_date', 'pickup_location', 'dropoff_location',
                'distance_miles', 'duration_minutes', 'fare_amount', 'tip_amount',
                'total_amount', 'payment_method', 'trip_status', 'customer_rating',
                'driver_rating'
            ]
        }

        # Save each dataset if it exists
        if new_customers:
            results.append(
                self.write_to_csv(self.FILENAMES['customers'], new_customers, field_mappings['customers'])
            )

        if new_drivers:
            results.append(
                self.write_to_csv(self.FILENAMES['drivers'], new_drivers, field_mappings['drivers'])
            )

        if new_vehicles:
            results.append(
                self.write_to_csv(self.FILENAMES['vehicles'], new_vehicles, field_mappings['vehicles'])
            )

        if trip_details:
            results.append(
                self.write_to_csv(self.FILENAMES['trips'], trip_details, field_mappings['trips'])
            )

        return all(results)

    def generate_summary_report(self):
        """Generate and display a summary report of all saved data"""
        if not self.report_data:
            print("No data generated to report")
            return ""

        headers = ["File", "Records", "Status", "Details"]

        # Add totals row
        total_records = sum(item['records'] for item in self.report_data)
        self.report_data.append({
            'file': 'TOTAL',
            'records': total_records,
            'status': '',
            'message': f"{len(self.report_data) - 1} files generated"
        })

        # Format report as table
        table_data = [
            [item['file'], item['records'], item['status'], item['message']]
            for item in self.report_data
        ]

        report = tabulate(
            table_data,
            headers=headers,
            tablefmt="fancy_grid",
            numalign="right",
            stralign="left"
        )

        # Add timestamp header
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"DATA GENERATION REPORT ({timestamp})"
        separator = "=" * len(header)

        full_report = f"\n{separator}\n{header}\n{separator}\n\n{report}\n"
        print(full_report)

        # Save report to file
        report_filename = os.path.join(
            self.output_path,
            f"generation_report.txt"
        )
        with open(report_filename, 'w') as f:
            f.write(full_report)

        print(f"Report saved to: {report_filename}")
        return full_report