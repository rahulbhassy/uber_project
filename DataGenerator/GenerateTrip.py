from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader
from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setVEnv
from DGFunctions import DataSaver, TripGenerator
import random
from faker import Faker
from typing import List, Any
from pyspark.sql import functions as F

setVEnv()
spark = create_spark_session()
fake = Faker()

def load_active_entities(table: str, status_column: str, active_status: str):
    """Load active entities from a Delta table"""
    getter = DataLakeIO(
        process='read',
        table=table,
        layer='raw',
        state='current',
        loadtype='full'
    )
    loader = DataLoader(
        path=getter.filepath(),
        filetype='delta',
        loadtype='full'
    )
    df = loader.LoadData(spark=spark)
    active_df = df.filter(F.col(status_column) == active_status)
    print(f"Loaded {active_df.count()} active {table} records")
    return active_df

def convert_to_dict_list(df, columns):
    """Convert Spark DataFrame to list of dictionaries with selected columns"""
    return [
        {col: row[col] for col in columns}
        for row in df.select(*columns).collect()
    ]

# 1. Load trip IDs
readuberfares = DataLakeIO(
    process='read',
    table='uberfares',
    layer='raw',
    state='current',
    loadtype='full'
)
reader = DataLoader(
    path=readuberfares.filepath(),
    filetype='delta',
    loadtype='full'
)
rows = reader.LoadData(spark=spark).select("trip_id").collect()

# Extract the value from each Row
trip_ids: List[Any] = [row.trip_id for row in rows]
print(f"Trip ids loaded with count: {len(trip_ids)}")

# 2. Load active entities
active_customers = load_active_entities("customerdetails", "membership_status", "Active")
active_drivers = load_active_entities("driverdetails", "status", "Active")
active_vehicles = load_active_entities("vehicledetails", "status", "Active")

# 3. Convert to dictionaries for trip generation
active_customers_list = convert_to_dict_list(active_customers, ["customer_id", "customer_name"])
active_drivers_list = convert_to_dict_list(active_drivers, ["driver_id", "driver_name"])
active_vehicles_list = convert_to_dict_list(active_vehicles, ["vehicle_no"])

# 4. Create a simple pool of active entities
class ActivePool:
    def __init__(self, customers, drivers, vehicles):
        self.active_customers = customers
        self.active_drivers = drivers
        self.active_vehicles = vehicles

active_pool = ActivePool(
    customers=active_customers_list,
    drivers=active_drivers_list,
    vehicles=active_vehicles_list
)

# 5. Generate trip details
trip_generator = TripGenerator(active_pool, fake)
trip_details = trip_generator.generate_trip_details(trip_ids)

# 6. Save trip details
data_saver = DataSaver()
data_saver.save_all(trip_details=trip_details)
data_saver.generate_summary_report()

print("Trip details generation completed successfully!")