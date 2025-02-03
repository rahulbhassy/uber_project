from pyspark.sql.functions import when, col, udf
from pyspark.sql.functions import to_timestamp, date_format
from pyspark.sql.types import BooleanType
from DataIngestion.DataLoader_UberFares import load_uberFares_data
from pyspark.sql.functions import hour, dayofweek, month
import os

# Set the PYSPARK_PYTHON environment variable
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

# Geospatial validation UDF
def validate_coordinates(lat, lon):
    return (40.4 <= lat <= 40.9) and (-74.3 <= lon <= -73.7)

def Clean_UberFares_Data():
    raw_df = load_uberFares_data()
    coord_udf = udf(validate_coordinates, BooleanType())

    cleaned_df = raw_df \
        .filter( "pickup_latitude IS NOT NULL AND pickup_longitude IS NOT NULL AND dropoff_latitude IS NOT NULL AND dropoff_longitude IS NOT NULL") \
        .filter(coord_udf(col("pickup_latitude"),col("pickup_longitude"))) \
        .withColumn("passenger_count",
            when(col("passenger_count").isNull(),1)
            .otherwise(col("passenger_count").cast("integer"))) \
        .dropDuplicates(["trip_id"]) \
        .filter(col("fare_amount") > 2.5) \
        .withColumn("date",date_format("pickup_datetime", "yyyy-MM-dd"))

    return cleaned_df

def Clean_Add_Temporal_Features():
    cleaned_df = Clean_UberFares_Data()
    time_features_data = cleaned_df \
        .withColumn("pickup_hour",hour(col("pickup_datetime"))) \
        .withColumn("pickup_day",dayofweek(col("pickup_datetime"))) \
        .withColumn("pickup_month",month(col("pickup_datetime"))) \
        .filter(col("date").isNotNull())

    return time_features_data

