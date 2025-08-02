
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import math
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader
from datetime import datetime


class WeatherAPI:
    def __init__(self, schema):
        self.schema = schema
        print("WeatherAPI initialized with schema:", str(schema))

    def safe_avg(self, vals):
        clean = [v for v in vals if v is not None]
        return sum(clean) / len(clean) if len(clean) >= 12 else None

    def _safe_mode(self, vals):
        from collections import Counter
        clean = [v for v in vals if v is not None]
        if len(clean) < 12:
            return None
        return Counter(clean).most_common(1)[0][0]

    def fetch_weather(self, date: str, lat: float, lon: float):
        import requests
        """
        Fetch hourly data for exactly one date,
        then reduce it to a daily summary.
        """
        try:
            print(f"\nFetching weather for {date}...")
            url = (
                f"https://archive-api.open-meteo.com/v1/archive"
                f"?latitude={lat}&longitude={lon}"
                f"&start_date={date}&end_date={date}"
                "&hourly=temperature_2m,precipitation,weather_code,"
                "relative_humidity_2m,wind_speed_10m,cloudcover,snowfall"
                "&timezone=auto"
            )

            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                print(f" HTTP ERROR {resp.status_code} for {date}")
                return {
                    "date": date,
                    "avg_temp": None,
                    "precipitation": None,
                    "weather_code": None,
                    "avg_humidity": None,
                    "avg_wind_speed": None,
                    "avg_cloud_cover": None,
                    "avg_snow_fall": None
                }

            hourly = resp.json().get("hourly", {})
            times = hourly.get("time", [])
            if not times:
                print(f" No hourly data for {date}")
                # return same shape but empty
                return {
                    "date": date,
                    "avg_temp": None,
                    "precipitation": None,
                    "weather_code": None,
                    "avg_humidity": None,
                    "avg_wind_speed": None,
                    "avg_cloud_cover": None,
                    "avg_snow_fall": None
                }

            # All indices for that single date
            idxs = list(range(len(times)))
            def extract(param, default=0):
                vals = hourly.get(param, [])
                return [vals[i] for i in idxs] if vals else [default] * len(idxs)

            # Gather lists
            temps = extract("temperature_2m", None)
            prcp  = extract("precipitation", 0)
            wcode = extract("weather_code", None)
            rh    = extract("relative_humidity_2m", None)
            wspd  = extract("wind_speed_10m", None)
            cloud = extract("cloudcover", None)
            snow  = extract("snowfall", 0)

            # Summarize
            avg_temp    = self.safe_avg(temps)
            total_prcp  = sum(v for v in prcp if v is not None)
            mode_code   = self._safe_mode(wcode)
            avg_hum     = self.safe_avg(rh)
            avg_wind    = self.safe_avg(wspd)
            avg_cloud   = self.safe_avg(cloud)
            avg_snow    = self.safe_avg(snow)

            print(f" SUCCESS for {date}: AvgTemp={avg_temp}, TotalPrecip={total_prcp}, Code={mode_code}")

            return {
                "date": datetime.strptime(date, "%Y-%m-%d").date(),
                "avg_temp": avg_temp,
                "precipitation": total_prcp,
                "weather_code": mode_code,
                "avg_humidity": avg_hum,
                "avg_wind_speed": avg_wind,
                "avg_cloud_cover": avg_cloud,
                "avg_snow_fall": avg_snow
            }

        except Exception as e:
            print(f" EXCEPTION for {date}: {str(e)[:100]}")
            return {
                "date": date,
                "avg_temp": None,
                "precipitation": None,
                "weather_code": None,
                "avg_humidity": None,
                "avg_wind_speed": None,
                "avg_cloud_cover": None,
                "avg_snow_fall": None
            }

    # Function to enrich Uber data with weather data
    def harmonise(self, data: DataFrame, spark: SparkSession):

    def enrich(
            self,
            data: DataFrame,
            spark: SparkSession
    ):
        from pyspark.sql.functions import to_date
        from concurrent.futures import ThreadPoolExecutor
        from pyspark.sql import Row

        print("\n" + "=" * 80)
        print("STARTING DATA ENRICHMENT PROCESS")
        print("=" * 80)

        # Get distinct dates
        distinct_dates = [row.date for row in data.select("date").distinct().collect()]
        total_dates = len(distinct_dates)
        print(f"\n FOUND {total_dates} DISTINCT DATES IN DATASET")

        # Initialize tracking structures
        successful_data = {}
        failed_dates = set(distinct_dates)
        print(f" INITIALIZING: All {total_dates} dates marked for first attempt")

        # Retry up to 3 times
        for attempt in range(3):
            if not failed_dates:
                print(f"\n ALL DATES SUCCEEDED! Skipping attempt {attempt + 1}")
                break

            print("\n" + "-" * 60)
            print(f" ATTEMPT #{attempt + 1} STARTING: Processing {len(failed_dates)} failed dates")
            print("-" * 60)

            # Process failed dates in parallel
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(self.fetch_weather, date, 40.7128, -74.0060)
                           for date in failed_dates]
                results = [f.result() for f in futures]

            # Process results
            current_failed = set()
            success_count = 0
            for result in results:
                date = result["date"]
                if result["avg_temp"] is not None:
                    successful_data[date] = result
                    success_count += 1
                else:
                    current_failed.add(date)

            # Update failed dates for next attempt
            failed_dates = current_failed
            print("\n" + "-" * 60)
            print(f" ATTEMPT #{attempt + 1} RESULTS:")
            print(f"   SUCCEEDED: {success_count} dates")
            print(f"   FAILED:    {len(failed_dates)} dates")
            print("-" * 60)

        # Final status report
        final_success = len(successful_data)
        final_failures = total_dates - final_success

        print("\n" + "=" * 80)
        print("FINAL RETRY STATUS")
        print("=" * 80)
        print(f"  TOTAL DATES:       {total_dates}")
        print(f"  SUCCESSFUL FETCH:  {final_success} ({final_success / total_dates:.1%})")
        print(f"  FAILED FETCH:      {final_failures} ({final_failures / total_dates:.1%})")

        if final_failures > 0:
            print("\n FINAL FAILED DATES:")
            for i, date in enumerate(sorted(failed_dates), 1):
                print(f"  {i}. {date}")

        # Combine successful results with final failed dates
        weather_list = []
        for date in distinct_dates:
            if date in successful_data:
                weather_list.append(successful_data[date])
            else:
                weather_list.append({"date": date, "avg_temp": None, "precipitation": None})

        # Create DataFrame
        print("\nBuilding weather DataFrame...")
        rows = [Row(**item) for item in weather_list]
        weather_df = spark.createDataFrame(rows, schema=self.schema)

        # Cast date to DateType and join
        weather_df = weather_df.withColumn("date", to_date("date", "yyyy-MM-dd"))
        enriched_weather = data.join(weather_df, on="date", how="left")

        # Filter out rows with missing weather data
        final_count = enriched_weather.filter(
            (enriched_weather.avg_temp.isNotNull()) &
            (enriched_weather.precipitation.isNotNull())
        ).count()

        print("\n" + "=" * 80)
        print("ENRICHMENT COMPLETE")
        print("=" * 80)
        print(f"  INITIAL ROWS:    {data.count()}")
        print(f"  ENRICHED ROWS:   {final_count}")
        print(f"  FILTERED ROWS:   {data.count() - final_count} (missing weather data)")

        return enriched_weather.filter(
            (enriched_weather.avg_temp.isNotNull()) &
            (enriched_weather.precipitation.isNotNull())
        )


import math
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


class Distance:
    def __init__(self):
        self.R = 6371  # Radius of Earth in kilometers
        print(" Distance calculator initialized | Earth radius: 6371 km")

    def haversine(self, lat1, lon1, lat2, lon2):
        try:
            # Convert to radians
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

            # Haversine calculation
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            distance = float(f"{self.R * c:.2f}")
            return distance

        except Exception as e:
            print(f" Error in distance calculation: {str(e)[:100]}")
            return None

    def enrich(self, data: DataFrame):
        print("\n" + "=" * 80)
        print("STARTING DISTANCE CALCULATION")
        print("=" * 80)
        print(f" Row count before enrichment: {data.count()}")

        # Register UDF
        haversine_udf = udf(self.haversine, DoubleType())
        print(" Registered Haversine UDF")

        # Add distance column
        data = data.withColumn("distance_km", haversine_udf(
            data.pickup_latitude, data.pickup_longitude,
            data.dropoff_latitude, data.dropoff_longitude
        ))

        # Analyze results
        null_distances = data.filter(data.distance_km.isNull()).count()
        valid_distances = data.count() - null_distances
        print(f"\n Distance calculation results:")
        print(f"   Valid distances:   {valid_distances} rows")
        print(f"   Null distances:    {null_distances} rows")
        print(f"   Total rows:        {data.count()} rows")
        return data


class PreHarmonizer:
    _KEY_COLUMN = "trip_id"

    def __init__(self, currentio: DataLakeIO):
        self.currentio = currentio
        print(f" PreHarmonizer initialized | Key column: {self._KEY_COLUMN}")
        print(f"   Data source: {currentio.filepath()}")

    def preharmonize(self, sourcedata: DataFrame, spark: SparkSession):
        print("\n" + "=" * 80)
        print("STARTING PRE-HARMONIZATION PROCESS")
        print("=" * 80)

        # Load current keys
        print(" Loading current dataset keys...")
        dataloader = DataLoader(
            path=self.currentio.filepath(),
            filetype='delta'
        )
        current_keys = dataloader.LoadData(spark)

        # Show key metrics
        print(f" Current keys loaded: {current_keys.count()} records")
        print(f" Source data: {sourcedata.count()} records")

        # Perform anti-join
        print(f"\n Filtering source data using left anti-join on '{self._KEY_COLUMN}'...")
        filtered_data = sourcedata.join(
            current_keys,
            on=self._KEY_COLUMN,
            how="left_anti"
        )

        # Calculate results
        removed_count = sourcedata.count() - filtered_data.count()
        retention_rate = filtered_data.count() / sourcedata.count() * 100

        print("\n Pre-harmonization results:")
        print(f"   Source rows:          {sourcedata.count()}")
        print(f"   Removed duplicates:   {removed_count}")
        print(f"   Filtered rows:        {filtered_data.count()}")
        print(f"   Retention rate:       {retention_rate:.2f}%")

        if filtered_data.count() > 0:
            sample_keys = filtered_data.select(self._KEY_COLUMN).limit(5).collect()
            print(f"\n Sample new keys:")
            for i, row in enumerate(sample_keys, 1):
                print(f"  {i}. {row[self._KEY_COLUMN]}")

        print("=" * 80 + "\n")
        return filtered_data


