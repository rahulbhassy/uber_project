from config.spark_config import create_spark_session
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType


def load_uberFares_data():
    spark = create_spark_session()
    schema = StructType([
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
    filepath = 'C:/Users/HP/uber_project/Data/UberFaresData/uber.csv'
    UberData = spark.read.csv(filepath,schema=schema,header=True)

    return UberData
