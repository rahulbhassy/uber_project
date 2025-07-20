import csv
import random
from faker import Faker
from datetime import datetime, timedelta
import os

# Define base directory
base_path = r'/Data/DataSource'

# Initialize Faker for synthetic data generation
fake = Faker()

# Ensure the directory exists
os.makedirs(base_path, exist_ok=True)

# Read trip IDs from the file
with open(os.path.join(base_path, 'tripid.txt'), 'r') as file:
    trip_ids = [line.strip() for line in file.readlines()]

# Generate driver details (100 drivers)
driver_details = []
experience_levels = ['Trainee', 'Junior', 'Senior', 'Expert', 'Master']
driver_statuses = ['Active', 'Active', 'Active', 'Active','Active', 'On Leave', 'Inactive']  # Weighted towards Active

for driver_id in range(1000, 1100):  # 100 drivers
    hire_date = fake.date_between(start_date='-5y', end_date='today')
    driver_details.append({
        'driver_id': driver_id,
        'driver_name': fake.name(),
        'license_no': fake.unique.bothify(text='??######').upper(),
        'phone_no': fake.numerify(text='+1##########'),
        'email': fake.email(),
        'address': fake.address().replace('\n', ', '),
        'date_of_birth': fake.date_of_birth(minimum_age=21, maximum_age=65),
        'hire_date': hire_date,
        'experience_level': random.choice(experience_levels),
        'rating': round(random.uniform(3.5, 5.0), 1),
        'status': random.choice(driver_statuses),
        'emergency_contact': fake.phone_number()
    })

# Generate customer details (500 customers)
customer_details = []
customer_types = ['Regular', 'Premium', 'Corporate', 'Student']
membership_statuses = ['Active', 'Active', 'Active','Active', 'Inactive', 'Suspended']  # Weighted towards Active

for customer_id in range(2000, 2500):  # 500 customers
    registration_date = fake.date_between(start_date='-5y', end_date='today')
    customer_details.append({
        'customer_id': customer_id,
        'customer_name': fake.name(),
        'phone_no': fake.numerify(text='+1##########'),
        'email': fake.email(),
        'address': fake.address().replace('\n', ', '),
        'date_of_birth': fake.date_of_birth(minimum_age=16, maximum_age=80),
        'registration_date': registration_date,
        'customer_type': random.choice(customer_types),
        'membership_status': random.choice(membership_statuses),
        'rating': round(random.uniform(3.0, 5.0), 1),
        'total_trips': random.randint(1, 500),
        'preferred_payment': random.choice(['Credit Card', 'Debit Card', 'Digital Wallet', 'Cash'])
    })

# Generate vehicle details (35 vehicles)
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

vehicle_details = []
for _ in range(35):  # 35 vehicles
    model = random.choice(vehicle_models)
    year = random.randint(2015, 2024)

    # Determine fuel type based on model
    if 'Tesla' in model:
        fuel_type = 'Electric'
    elif random.random() < 0.2:  # 20% chance of hybrid
        fuel_type = 'Hybrid'
    else:
        fuel_type = random.choice(['Gasoline', 'Diesel'])

    vehicle_details.append({
        'vehicle_no': fake.unique.license_plate(),
        'model': model,
        'year': year,
        'color': fake.color_name(),
        'vehicle_type': random.choice(vehicle_types),
        'fuel_type': fuel_type,
        'seating_capacity': random.choice([4, 5, 7, 8]),
        'mileage': random.randint(15000, 150000),
        'insurance_expiry': fake.date_between(start_date='today', end_date='+2y'),
        'last_maintenance': fake.date_between(start_date='-6m', end_date='today'),
        'status': random.choice(vehicle_statuses),
        'registration_state': fake.state_abbr()
    })

# Create trip_details with more realistic data
trip_details = []
trip_statuses = ['Completed', 'Completed', 'Completed', 'Completed', 'Cancelled', 'No Show']
payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet', 'Corporate Account']

# Get active entities (ensure at least one exists)
active_drivers = [d for d in driver_details if d['status'] == 'Active'] or driver_details
active_customers = [c for c in customer_details if c['membership_status'] == 'Active'] or customer_details
active_vehicles = [v for v in vehicle_details if v['status'] == 'Active'] or vehicle_details

for trip_id in trip_ids:
    driver = random.choice(active_drivers)
    customer = random.choice(active_customers)
    vehicle = random.choice(active_vehicles)

    # Generate realistic trip data
    trip_date = fake.date_time_between(start_date='-1y', end_date='now')
    distance = round(random.uniform(0.5, 50.0), 2)  # Distance in miles
    duration = random.randint(5, 120)  # Duration in minutes
    base_fare = round(distance * random.uniform(1.5, 3.0), 2)

    trip_details.append({
        'trip_id': trip_id,
        'driver_id': driver['driver_id'],
        'customer_id': customer['customer_id'],
        'vehicle_no': vehicle['vehicle_no'],
        'driver_name': driver['driver_name'],
        'customer_name': customer['customer_name'],
        'tip_amount': round(base_fare * random.uniform(0, 0.25), 2),
        'payment_method': random.choice(payment_methods),
        'trip_status': random.choice(trip_statuses),
        'customer_rating': random.choice([None, 3, 4, 4, 5, 5, 5]),  # Some trips have no rating
        'driver_rating': random.choice([None, 3, 4, 4, 5, 5, 5])
    })


# Write all data to CSV files
def write_to_csv(filename, data, fieldnames):
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        return True
    except Exception as e:
        print(f"Error writing {filename}: {str(e)}")
        return False

# Save driverdetails.csv
driver_success = write_to_csv(os.path.join(base_path, 'driverdetails.csv'), driver_details,
             ['driver_id', 'driver_name', 'license_no', 'phone_no', 'email', 'address',
              'date_of_birth', 'hire_date', 'experience_level', 'rating', 'status', 'emergency_contact'])

# Save customerdetails.csv
customer_success = write_to_csv(os.path.join(base_path, 'customerdetails.csv'), customer_details,
             ['customer_id', 'customer_name', 'phone_no', 'email', 'address', 'date_of_birth',
              'registration_date', 'customer_type', 'membership_status', 'rating', 'total_trips', 'preferred_payment'])

# Save vehicledetails.csv
vehicle_success = write_to_csv(os.path.join(base_path, 'vehicledetails.csv'), vehicle_details,
             ['vehicle_no', 'model', 'year', 'color', 'vehicle_type', 'fuel_type', 'seating_capacity',
              'mileage', 'insurance_expiry', 'last_maintenance', 'status', 'registration_state'])

# Save tripdetails.csv
trip_success = write_to_csv(os.path.join(base_path, 'tripdetails.csv'), trip_details,
             ['trip_id', 'driver_id', 'customer_id', 'vehicle_no', 'driver_name', 'customer_name',
              'tip_amount','payment_method', 'trip_status','customer_rating', 'driver_rating'])


print("\nCSV files generation status:")
print(f"- driverdetails.csv: {'Success' if driver_success else 'Failed'} ({len(driver_details)} records)")
print(f"- customerdetails.csv: {'Success' if customer_success else 'Failed'} ({len(customer_details)} records)")
print(f"- vehicledetails.csv: {'Success' if vehicle_success else 'Failed'} ({len(vehicle_details)} records)")
print(f"- tripdetails.csv: {'Success' if trip_success else 'Failed'} ({len(trip_details)} records)")

# Print active counts for debugging
print("\nActive counts:")
print(f"- Active drivers: {len(active_drivers)}")
print(f"- Active customers: {len(active_customers)}")
print(f"- Active vehicles: {len(active_vehicles)}")