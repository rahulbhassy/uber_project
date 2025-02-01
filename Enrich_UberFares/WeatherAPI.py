import requests
from pyspark.sql.functions import date_format
from pyspark.sql import Row
from concurrent.futures import ThreadPoolExecutor
from config.spark_config import create_spark_session
import os

# Set the PYSPARK_PYTHON environment variable
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

def fetch_weather(date,lat,lon):
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={date}&end_date={date}&hourly=temperature_2m,precipitation&timezone=auto"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        # Extract hourly temperatures and precipitation
        temperatures = data["hourly"]["temperature_2m"]
        precipitation = data["hourly"]["precipitation"]

        # Calculate average temperature
        avg_temp = sum(temperatures) / len(temperatures)
        formatted_temp = float(f"{avg_temp:.2f}")

        # Calculate total daily precipitation
        total_precipitation = sum(precipitation)
        total_precipitation = float(f"{total_precipitation:.2f}")

        return {"date":date,"avg_temp":formatted_temp,"precipitation":total_precipitation}
    else:
        return {"date":None,"avg_temp":None,"precipitation":None}

def enrich_with_weather(uberData):
    dates = uberData.select(date_format(uberData.pickup_datetime,"yyyy-MM-dd").alias("date")).distinct().collect()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for row in dates:
            futures.append(executor.submit(
                fetch_weather, row.date, 40.7128, -74.0060  # NYC coordinates
            ))

        weather_data = [f.result() for f in futures]

    spark = create_spark_session()
    weather_df = spark.createDataFrame(Row(**item) for item in weather_data)
    return uberData.join(weather_df,
                   on=date_format(uberData.pickup_datetime, "yyyy-MM-dd") == weather_df.date,
                   how="left")


