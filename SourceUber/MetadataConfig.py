
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

def sourceschema(sourcedefinition: str):
    if sourcedefinition.lower() == "uberfares":
        return StructType([
        StructField("trip_id",StringType(),False),
        StructField("dropoff_datetime",TimestampType(),False),
        StructField("fare_amount", DoubleType(), False),
        StructField("pickup_datetime", TimestampType(), False),
        StructField("pickup_longitude", DoubleType(), False),
        StructField("pickup_latitude", DoubleType(), False),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True)
        ])

def filepath(process):
    if process == "load":
        return 'C:/Users/HP/uber_project/Data/UberFaresData/uber.csv'
    elif process == "write" or "read":
        return 'C:/Users/HP/uber_project/Data/Cleaned_UberFares/UberFares.csv'


def filetype(sourcedefinition: str):
    if sourcedefinition =="uberfares":
        return "csv"
    elif sourcedefinition == "destination":
        return "delta"