import os
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType, DateType
)

# -- SCHEMA FACTORY --
def sourceschema(sourcedefinition: str) -> StructType:
    sd = sourcedefinition.lower()

    if sd == "driverdetails":
        return StructType([
            StructField("driver_id", StringType(), False),
            StructField("driver_name", StringType(), False),
            StructField("license_no", StringType(), False),
            StructField("phone_no", StringType(), False),
            StructField("email", StringType(), False),
            StructField("address", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("hire_date", DateType(), True),
            StructField("experience_level", StringType(), True),
            StructField("rating", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("emergency_contact", StringType(), True),
        ])

    if sd == "customerdetails":
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("phone_no", StringType(), False),
            StructField("email", StringType(), False),
            StructField("address", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("registration_date", DateType(), True),
            StructField("customer_type", StringType(), True),
            StructField("membership_status", StringType(), True),
            StructField("rating", DoubleType(), True),
            StructField("total_trips", IntegerType(), True),
            StructField("preferred_payment", StringType(), True),
        ])

    if sd == "vehicledetails":
        return StructType([
            StructField("vehicle_no", StringType(), False),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("color", StringType(), True),
            StructField("vehicle_type", StringType(), True),
            StructField("fuel_type", StringType(), True),
            StructField("seating_capacity", IntegerType(), True),
            StructField("mileage", IntegerType(), True),
            StructField("insurance_expiry", DateType(), True),
            StructField("last_maintenance", DateType(), True),
            StructField("status", StringType(), True),
            StructField("registration_state", StringType(), True),
        ])

    raise ValueError(f"No schema defined for '{sourcedefinition}'")
