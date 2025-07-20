import os
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType
)


# -- SCHEMA FACTORY --
def sourceschema(sourcedefinition: str) -> StructType:
    sd = sourcedefinition.lower()
    if sd == "uberfares":
        return StructType([
            StructField("trip_id", StringType(), False),
            StructField("dropoff_datetime", TimestampType(), False),
            StructField("fare_amount", DoubleType(), False),
            StructField("pickup_datetime", TimestampType(), False),
            StructField("pickup_longitude", DoubleType(), False),
            StructField("pickup_latitude", DoubleType(), False),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("dropoff_latitude", DoubleType(), True),
            StructField("passenger_count", IntegerType(), True),
        ])

    if sd == "tripdetails":
        return StructType([
            StructField("trip_id",StringType(),False),
            StructField("driver_id",StringType(),False),
            StructField("customer_id",StringType(),False),
            StructField("vehicle_no",StringType(),False),
            StructField("pickup_location",StringType(),True),
            StructField("dropoff_location",StringType(),True),
            StructField("tip_amount",DoubleType(),True),
            StructField("trip_status",StringType(),False),
            StructField("customer_rating",DoubleType(),True),
            StructField("driver_rating",DoubleType(),True)

        ])
    raise ValueError(f"No schema defined for '{sourcedefinition}'")



