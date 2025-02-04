import requests
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import to_date
from config.spark_config import create_spark_session
import os
# Set the PYSPARK_PYTHON environment variable
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"
# Initialize Spark session
spark = create_spark_session()

# Define the weather schema
weather_schema = StructType([
    StructField("date", StringType(), nullable=False),   # We'll cast to DateType later if desired.
    StructField("avg_temp", DoubleType(), nullable=True),
    StructField("precipitation", DoubleType(), nullable=True)
])

# Function to fetch weather data for a given date and location
def fetch_weather(date, lat, lon):
    try:
        url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={lat}&longitude={lon}&start_date={date}&end_date={date}"
            f"&hourly=temperature_2m,precipitation&timezone=auto"
        )
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            temperatures = data["hourly"]["temperature_2m"]
            precipitation = data["hourly"]["precipitation"]

            # Calculate average temperature and total precipitation
            avg_temp = sum(temperatures) / len(temperatures)
            total_precipitation = sum(precipitation)

            # Print type information for debugging
            print(f"For date {date}:")
            print(f"  temperature type (first value): {type(temperatures[0])}  |  avg_temp type: {type(avg_temp)}")
            print(f"  precipitation type (first value): {type(precipitation[0])}  |  total_precipitation type: {type(total_precipitation)}")

            return {"date": date, "avg_temp": avg_temp, "precipitation": total_precipitation}
        else:
            print(f"Error fetching data for {date}: {response.status_code}")
            return {"date": date, "avg_temp": None, "precipitation": None}
    except Exception as e:
        print(f"Error fetching weather data for date {date}: {e}")
        return {"date": date, "avg_temp": None, "precipitation": None}

# ----------------- User Input and Threaded API Calls -----------------

# Get user inputs for latitude, longitude, and 10 dates
lat = input("Enter latitude: ").strip()
lon = input("Enter longitude: ").strip()

# Here is an example input for 10 dates:
# 2020-01-01,2020-01-02,2020-01-03,2020-01-04,2020-01-05,2020-01-06,2020-01-07,2020-01-08,2020-01-09,2020-01-10
dates_input = input("Enter 10 dates (comma separated, e.g., 2020-01-01,...,2020-01-10): ").strip()
dates = [date.strip() for date in dates_input.split(",") if date.strip()]

# Use ThreadPoolExecutor to fetch weather data concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_weather, date, lat, lon) for date in dates]
    weather_data = [future.result() for future in futures]

# Print the Python types for the fetched data
for wd in weather_data:
    print(f"Fetched weather for date {wd['date']}:")
    print(f"  avg_temp type: {type(wd['avg_temp'])}")
    print(f"  precipitation type: {type(wd['precipitation'])}")

# ----------------- Creating and Examining a Spark DataFrame -----------------

# Create a DataFrame with the defined schema using the weather data
rows = [Row(**item) for item in weather_data]
weather_df = spark.createDataFrame(rows, schema=weather_schema)

# Print the Spark DataFrame schema (shows Spark data types)
print("\nSpark DataFrame Schema BEFORE converting 'date' to DateType:")
weather_df.printSchema()

# Optionally, cast the "date" column from StringType to DateType
weather_df = weather_df.withColumn("date", to_date("date", "yyyy-MM-dd"))

print("\nSpark DataFrame Schema AFTER converting 'date' to DateType:")
weather_df.printSchema()

# Show the DataFrame content for verification (optional)
weather_df.show(truncate=False)
