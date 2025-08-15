from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader
from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setVEnv
from DGFunctions import DataSaver
import random
from faker import Faker
from datetime import date, timedelta

def generate_one_vehicle_per_driver(driver_ids):
    fake = Faker()
    new_vehicles = []
    vehicle_models = [
        'Toyota Camry', 'Honda Civic', 'Ford F-150', 'Chevrolet Silverado',
        'Tesla Model 3', 'Nissan Altima', 'Hyundai Elantra', 'BMW 3 Series',
        'Audi A4', 'Mercedes-Benz C-Class', 'Volkswagen Jetta', 'Subaru Outback',
        'Mazda CX-5', 'Jeep Wrangler', 'GMC Sierra', 'Ram 1500',
        'Tesla Model S', 'Lexus ES', 'Acura TLX', 'Infiniti Q50',
        # New York taxi‐specific models
        'Toyota Prius', 'Nissan NV200', 'Ford Transit Connect', 'Chevrolet Impala',
        'Honda Accord', 'Toyota Sienna', 'Chrysler Pacifica', 'Ford Escape',
        'Kia Optima', 'Hyundai Sonata', 'Lincoln Town Car', 'Cadillac XTS',
        'Mercedes-Benz Sprinter'
    ]
    vehicle_types = ['Sedan', 'SUV', 'Hatchback', 'Pickup Truck', 'Electric', 'Luxury', 'Minivan', 'Van']
    fuel_types = ['Gasoline', 'Electric', 'Hybrid', 'Diesel']
    vehicle_statuses = ['Active','Active', 'Active', 'Active',
                        'Active', 'Active','Active', 'Active',
                        'Active','Maintenance', 'Out of Service']

    for driver_id in driver_ids:
        model = random.choice(vehicle_models)
        year = random.randint(2009, 2010)

        # Fuel logic
        if 'Tesla' in model:
            fuel_type = 'Electric'
        elif 'Prius' in model or random.random() < 0.2:
            fuel_type = 'Hybrid'
        else:
            # fallback to any fuel type
            fuel_type = random.choice(fuel_types)

        new_vehicles.append({
            'vehicle_no': fake.unique.license_plate(),
            'driver_id': driver_id,
            'model': model,
            'year': year,
            'color': fake.color_name(),
            'vehicle_type': random.choice(vehicle_types),
            'fuel_type': fuel_type,
            'seating_capacity': random.choice([4, 5, 6, 7, 8]),
            'mileage': random.randint(15000, 200000),
            'insurance_expiry': fake.date_between(
                start_date=date(2012, 1, 1),
                end_date=date(2017,12,31)
            ).strftime('%Y-%m-%d'),
            'last_maintenance': fake.date_between(
                start_date=date(2009, 1, 1),
                end_date=date(2015, 12, 31)
            ).strftime('%Y-%m-%d'),
            'status': random.choice(vehicle_statuses),
            'registration_state': fake.state_abbr()
        })

    print(f"Generated {len(new_vehicles)} vehicles for {len(driver_ids)} drivers")
    return new_vehicles

# ─── Your existing setup/IO/loading ────────────────────────────────────────────
setVEnv()
spark = create_spark_session()

readio = DataLakeIO(
    process='read',
    table='driverdetails',
    state='current',
    layer='raw',
    loadtype='full'
)
reader = DataLoader(
    path=readio.filepath(),
    filetype='delta',
    loadtype='full'
)
driverdf = reader.LoadData(spark=spark)
driver_ids = [row.driver_id for row in driverdf.select("driver_id").collect()]
print("Driver DataFrame loaded successfully.")

# ─── Generate one vehicle per driver ───────────────────────────────────────────
vehicledetails = generate_one_vehicle_per_driver(driver_ids)

saver = DataSaver()
saver.save_all(vehicledetails)