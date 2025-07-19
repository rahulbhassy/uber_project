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
    raise ValueError(f"No schema defined for '{sourcedefinition}'")

