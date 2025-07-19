
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col, to_date
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import Row
import math
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

class WeatherAPI:

    def __init__(self,schema):
        self.schema = schema

    # Function to fetch weather data
    def fetch_weather(self,date, lat, lon):
        import requests
        try:
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={date}&end_date={date}&hourly=temperature_2m,precipitation&timezone=auto"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                temperatures = data["hourly"]["temperature_2m"]
                precipitation = data["hourly"]["precipitation"]
                avg_temp = sum(temperatures) / len(temperatures)
                total_precipitation = sum(precipitation)
                print(f"Fetched data for date {date} succesfully. ")
                return {"date": date, "avg_temp": avg_temp, "precipitation": total_precipitation}
            else:
                print(f"Fetched data for date {date} succesfully. ")
                return {"date": date, "avg_temp": None, "precipitation": None}

        except Exception as e:
            print(f"Error fetching weather data for date {date}: {e}")
            return {"date": date, "avg_temp": None, "precipitation": None}

    # Function to enrich Uber data with weather data
    def enrich(
        self,
        data: DataFrame,
        spark: SparkSession
    ):
        dates = data.select("date").distinct().collect()
        # Fetch weather data
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.fetch_weather, row.date, 40.7128, -74.0060) for row in dates]
            weather_data = [f.result() for f in futures]
        # Create DataFrame with string-based schema

        rows = [Row(**item) for item in weather_data]
        weather_df = spark.createDataFrame(rows, schema=self.schema)

        # Cast date to DateType and join
        weather_df = weather_df.withColumn("date", to_date("date", "yyyy-MM-dd"))
        enriched_weather = data.join(weather_df, on="date", how="left")
        return enriched_weather.filter(
            (enriched_weather.avg_temp.isNotNull()) &
            (enriched_weather.precipitation.isNotNull())
        )


class Distance:
    def __init__(self):
        self.R = 6371  # Radius of Earth in kilometers

    def haversine(self,lat1, lon1, lat2, lon2):

        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = float(f"{self.R * c : .2f}")  # Distance in kilometers

        return distance

    def enrich(
            self,
            data: DataFrame
    ):
        haversine_udf = udf(self.haversine, DoubleType())
        data = data.withColumn("distance_km", haversine_udf(
            data.pickup_latitude, data.pickup_longitude,
            data.dropoff_latitude, data.dropoff_longitude
        ))
        return data


