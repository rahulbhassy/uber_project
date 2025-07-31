
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import math
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader


class WeatherAPI:

    def __init__(self, schema):
        self.schema = schema
        print("WeatherAPI initialized with schema:", str(schema))

    # Function to fetch weather data with detailed logging
    def fetch_weather(self, date, lat, lon):
        import requests
        try:
            print(f"\nFetching weather for {date}...")
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={date}&end_date={date}&hourly=temperature_2m,precipitation&timezone=auto"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                temperatures = data["hourly"]["temperature_2m"]
                precipitation = data["hourly"]["precipitation"]
                avg_temp = sum(temperatures) / len(temperatures)
                total_precipitation = sum(precipitation)
                print(f" SUCCESS for {date}: Temp={avg_temp:.2f}C, Precip={total_precipitation}mm")
                return {"date": date, "avg_temp": avg_temp, "precipitation": total_precipitation}
            else:
                print(f" HTTP ERROR for {date}: Status {response.status_code}")
                return {"date": date, "avg_temp": None, "precipitation": None}

        except Exception as e:
            print(f" EXCEPTION for {date}: {str(e)[:100]}")
            return {"date": date, "avg_temp": None, "precipitation": None}

    # Function to enrich Uber data with weather data
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


