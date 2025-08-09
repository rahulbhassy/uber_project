from DGFunctions import DataSaver
import random
from faker import Faker
from datetime import datetime, date

# ─── Seed once for reproducibility ──────────────────────────────────────────────
Faker.seed(42)
fake = Faker()

def generate_drivers(num_new_drivers):
    new_drivers = []
    experience_levels = ['Trainee', 'Junior', 'Senior', 'Expert', 'Master']
    driver_statuses = [
        'Active', 'Active', 'Active', 'Active', 'Active',
        'Active', 'Active', 'Active', 'Active', 'Active',
        'On Leave', 'Inactive'
    ]

    # Define the date range explicitly
    hire_start = date(2009, 1, 1)
    hire_end   = date(2010, 12, 31)

    for i in range(num_new_drivers):
        driver_id = 1000 + i + 1

        # Use date_between_dates for explicit date bounds
        hire_date = fake.date_between_dates(date_start=hire_start, date_end=hire_end)

        new_drivers.append({
            'driver_id': driver_id,
            'driver_name': fake.name(),
            'license_no': fake.unique.bothify(text='??######').upper(),
            'phone_no': fake.numerify(text='+1##########'),
            'email': fake.email(),
            'address': fake.address().replace('\n', ', '),
            'date_of_birth': fake.date_of_birth(minimum_age=21, maximum_age=65),
            'hire_date': hire_date,
            'experience_level': random.choice(experience_levels),
            'rating': round(random.uniform(3.5, 5.0), 2),
            'status': random.choice(driver_statuses),
            'emergency_contact': fake.phone_number()
        })

    print(f"Generated {len(new_drivers)} drivers")
    return new_drivers

def generate_historical_drivers(num_drivers=500):
    """Generate historical driver data from scratch"""
    return generate_drivers(num_new_drivers=num_drivers)

def main():
    print("Starting historical driver generation...")

    NUM_DRIVERS = 250
    OUTPUT_FILENAME = "driverdetails.csv"

    # Generate drivers
    historical_drivers = generate_historical_drivers(num_drivers=NUM_DRIVERS)

    # Save to CSV
    saver = DataSaver()
    # You can override just the drivers filename:
    saver.FILENAMES['drivers'] = OUTPUT_FILENAME

    # Field mapping for driver data
    driver_fields = [
        'driver_id', 'driver_name', 'license_no', 'phone_no', 'email',
        'address', 'date_of_birth', 'hire_date', 'experience_level',
        'rating', 'status', 'emergency_contact'
    ]

    saver.write_to_csv(
        filename=OUTPUT_FILENAME,
        data=historical_drivers,
        fieldnames=driver_fields
    )

    saver.generate_summary_report()

    print(f"\nSuccessfully generated {len(historical_drivers)} historical drivers")
    print(f"Data saved to: {saver.output_path}/{OUTPUT_FILENAME}")

if __name__ == "__main__":
    main()
