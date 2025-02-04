from pyspark.sql.types import DoubleType
from Enrich_UberFares.haversine import haversine
from pyspark.sql.functions import udf
from Enrich_UberFares.WeatherAPI import enrich_with_weather
from config.spark_config import create_spark_session
import os
# Set the PYSPARK_PYTHON environment variable
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"


def enrichWeather():
    spark = create_spark_session()
    filepath = "C:/Users/HP/uber_project/Data/Cleaned_UberFares/UberFares.csv"
    uberData = spark.read.format("delta").load(filepath)
    enriched_uberData = enrich_with_weather(uberData)

    enriched_uberData.write \
        .format("delta") \
        .mode("overwrite") \
        .save("C:/Users/HP/uber_project/Data/Enriched_Weather_uberData/uberData.csv")

    return enriched_uberData

def enrichDistance(uberData):
    haversine_udf = udf(haversine,DoubleType())

    uberData = uberData.withColumn("distance_km", haversine_udf(
        uberData.pickup_latitude, uberData.pickup_longitude,
        uberData.dropoff_latitude, uberData.dropoff_longitude
    ))

    uberData.write \
        .format("delta") \
        .mode("overwrite") \
        .save("C:/Users/HP/uber_project/Data/Enriched_Distance_uberData/uberData.csv")

    return uberData

def enrichUberData():
    uberData = enrichWeather()
    enriched_UberData = enrichDistance(uberData)

    return enriched_UberData

uberData = enrichUberData()
uberData.show()
