import requests
from config.spark_config import create_spark_session
from pyspark.sql.functions import date_format, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import Row
import os
# Set the PYSPARK_PYTHON environment variable
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

# Initialize Spark session
spark = create_spark_session()

# Define the weather schema
weather_schema = StructType([
    StructField("date", StringType(), nullable=False),  # Accept string first
    StructField("avg_temp", DoubleType(), nullable=True),
    StructField("precipitation", DoubleType(), nullable=True)
])

# Function to fetch weather data
def fetch_weather(date, lat, lon):
    try:
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={date}&end_date={date}&hourly=temperature_2m,precipitation&timezone=auto"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            temperatures = data["hourly"]["temperature_2m"]
            precipitation = data["hourly"]["precipitation"]
            avg_temp = sum(temperatures) / len(temperatures)
            total_precipitation = sum(precipitation)
            return {"date": date, "avg_temp": avg_temp, "precipitation": total_precipitation}
        else:
            return {"date": date, "avg_temp": None, "precipitation": None}
    except Exception as e:
        print(f"Error fetching weather data for date {date}: {e}")
        return {"date": date, "avg_temp": None, "precipitation": None}

# Function to enrich Uber data with weather data
def enrich_with_weather(uberData):
    # Ensure date is parsed correctly in uberData
    uberData = uberData.withColumn("date", to_date("pickup_datetime", "yyyy-MM-dd"))

    # Get distinct dates
    dates = uberData.select("date").distinct().collect()

    # Fetch weather data
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_weather, row.date, 40.7128, -74.0060) for row in dates]
        weather_data = [f.result() for f in futures]

    # Create DataFrame with string-based schema
    rows = [Row(**item) for item in weather_data]
    weather_df = spark.createDataFrame(rows, schema=weather_schema)

    # Cast date to DateType and join
    weather_df = weather_df.withColumn("date", to_date("date", "yyyy-MM-dd"))
    enriched_df = uberData.join(weather_df, on="date", how="left")

    return enriched_df

