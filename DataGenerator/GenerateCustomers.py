from DGFunctions import DataSaver, DataGenerator
import random
from faker import Faker
from datetime import datetime, date


def generate_new_customers(num_new_customers=2000):
    Faker.seed(42)
    fake = Faker()
    new_customers = []
    customer_types = ['Regular', 'Premium', 'Corporate', 'Student']
    membership_statuses = ['Active', 'Active', 'Active', 'Active', 'Active', 'Active', 'Active',
                           'Active', 'Active', 'Active', 'Active', 'Active', 'Active', 'Active',
                           'Inactive',
                           'Suspended']

    for i in range(num_new_customers):
        customer_id = 20000 + i + 1
        start_date = date(2009, 1, 1)
        end_date = date(2012, 12, 31)
        registration_date = fake.date_between(start_date=start_date, end_date=end_date)

        new_customers.append({
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
            'preferred_payment': random.choice(['Credit Card', 'Debit Card', 'Digital Wallet', 'Cash'])
        })

    print(
        f"Generated {len(new_customers)} ")
    return new_customers

def generate_historical_customers(num_customers):
    """Generate historical customer data from scratch"""

    # Generate customers with historical registration dates
    customers = generate_new_customers(
        num_new_customers=num_customers
    )

    return customers


def main():
    print("Starting historical customer generation...")

    # Configuration
    NUM_CUSTOMERS = 10000
    OUTPUT_FILENAME = "customerdetails.csv"

    # Generate customers
    historical_customers = generate_historical_customers(
        num_customers=NUM_CUSTOMERS,
    )

    # Save to CSV
    saver = DataSaver()
    saver.FILENAMES = {'customers': OUTPUT_FILENAME}  # Custom filename

    # Create field mapping for customer data
    customer_fields = [
        'customer_id', 'customer_name', 'phone_no', 'email', 'address',
        'date_of_birth', 'registration_date', 'customer_type',
        'membership_status', 'rating', 'preferred_payment'
    ]

    saver.write_to_csv(OUTPUT_FILENAME, historical_customers, customer_fields)

    # Generate summary report
    saver.generate_summary_report()

    print(f"\nSuccessfully generated {len(historical_customers)} historical customers")
    print(f"Data saved to: {saver.output_path}/{OUTPUT_FILENAME}")


if __name__ == "__main__":
    main()