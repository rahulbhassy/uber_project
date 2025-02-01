from pyspark.sql.functions import when, col, udf
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
        .filter(coord_udf(col("pickup_latitude"),col("pickup_longitude"))) \
        .withColumn("passenger_count",
            when(col("passenger_count").isNull(),1)
            .otherwise(col("passenger_count").cast("integer"))) \
        .dropDuplicates(["trip_id"]) \
        .filter(col("fare_amount") > 2.5)

    return cleaned_df

def add_temporal_features():
    cleaned_df = load_uberFares_data()
    time_features_data = cleaned_df \
        .withColumn("pickup_hour",hour(col("pickup_datetime"))) \
        .withColumn("pickup_day",dayofweek(col("pickup_datetime"))) \
        .withColumn("pickup_month",month(col("pickup_datetime")))
    return time_features_data