from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import time

class WeatherAPI:
    def __init__(self, schema):
        self.schema = schema
        print("WeatherAPI initialized with schema:", str(schema))

    def safe_avg(self, vals):
        clean = [v for v in vals if v is not None]
        return sum(clean) / len(clean) if clean and len(clean) >= 12 else None

    def _safe_mode(self, vals):
        from collections import Counter
        clean = [v for v in vals if v is not None]
        if not clean or len(clean) < 12:
            return None
        return Counter(clean).most_common(1)[0][0]

    def fetch_weather_batch(self, start_date, end_date, lat, lon):
        import requests
        try:
            print(f"\nFetching weather for {start_date} to {end_date}...")
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}"
            url += f"&start_date={start_date}&end_date={end_date}"
            url += "&hourly=temperature_2m,precipitation,weather_code,relative_humidity_2m,wind_speed_10m,cloudcover,snowfall"
            url += "&timezone=auto"

            response = requests.get(url, timeout=10)
            print(f"URL: {url}")
            print(f"Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                hourly = data.get("hourly", {})

                # Diagnostic: Check available parameters
                print(f"Available parameters: {list(hourly.keys())}")

                # Get dates from timestamps
                times = hourly.get("time", [])
                dates = [t.split('T')[0] for t in times] if times else []

                if not dates:
                    print("WARNING: No time data found")
                    return []

                # Diagnostic: Show sample data
                if times:
                    print(f"Sample time data: {times[:5]}")

                results = []
                param_defaults = {
                    "temperature_2m": None,
                    "precipitation": 0,
                    "weather_code": None,
                    "relative_humidity_2m": None,
                    "wind_speed_10m": None,
                    "cloudcover": None,
                    "snowfall": 0
                }

                unique_dates = set(dates)
                print(f"Processing {len(unique_dates)} unique dates")

                for target_date in unique_dates:
                    indices = [i for i, d in enumerate(dates) if d == target_date]

                    # Diagnostic: Check if we have data for this date
                    if not indices:
                        print(f"No data found for date: {target_date}")
                        continue

                    # Extract data with safe defaults
                    extracted = {}
                    for param, default in param_defaults.items():
                        vals = hourly.get(param, [default] * len(times))
                        extracted[param] = [vals[i] for i in indices]

                    # Build daily summary
                    # Convert target_date string to datetime.date object
                    date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()

                    daily_data = {
                        "date": date_obj,  # Store as date object
                        "avg_temp": self.safe_avg(extracted["temperature_2m"]),
                        "precipitation": self.safe_avg(extracted["precipitation"]),
                        "weather_code": self._safe_mode(extracted["weather_code"]),
                        "avg_humidity": self.safe_avg(extracted["relative_humidity_2m"]),
                        "avg_wind_speed": self.safe_avg(extracted["wind_speed_10m"]),
                        "avg_cloud_cover": self.safe_avg(extracted["cloudcover"]),
                        "avg_snow_fall": self.safe_avg(extracted["snowfall"])
                    }
                    results.append(daily_data)

                print(f"Processed {len(results)} days")
                return results
            else:
                print(f"Error {response.status_code}: {response.text[:200]}")
                return []
        except Exception as e:
            print(f"Exception: {str(e)}")
            return []

    def fetch_weather_single(self, date: str, lat: float, lon: float):
        import requests
        try:
            print(f"\n[Single] Fetching weather for {date}...")
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}"
            url += f"&start_date={date}&end_date={date}"
            url += "&hourly=temperature_2m,precipitation,weather_code,relative_humidity_2m,wind_speed_10m,cloudcover,snowfall"
            url += "&timezone=auto"

            response = requests.get(url, timeout=10)
            print(f"URL: {url}")
            print(f"Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                hourly = data.get("hourly", {})
                times = hourly.get("time", [])
                if not times:
                    print(f"No hourly data for {date}")
                    return None

                # Extract the data for the day
                extracted = {}
                param_defaults = {
                    "temperature_2m": None,
                    "precipitation": 0,
                    "weather_code": None,
                    "relative_humidity_2m": None,
                    "wind_speed_10m": None,
                    "cloudcover": None,
                    "snowfall": 0
                }

                for param, default in param_defaults.items():
                    vals = hourly.get(param, [])
                    if not vals:
                        vals = [default] * len(times)
                    extracted[param] = vals

                # Build daily summary
                date_obj = datetime.strptime(date, "%Y-%m-%d").date()

                daily_data = {
                    "date": date_obj,
                    "avg_temp": self.safe_avg(extracted["temperature_2m"]),
                    "precipitation": self.safe_avg(extracted["precipitation"]),
                    "weather_code": self._safe_mode(extracted["weather_code"]),
                    "avg_humidity": self.safe_avg(extracted["relative_humidity_2m"]),
                    "avg_wind_speed": self.safe_avg(extracted["wind_speed_10m"]),
                    "avg_cloud_cover": self.safe_avg(extracted["cloudcover"]),
                    "avg_snow_fall": self.safe_avg(extracted["snowfall"])
                }
                return daily_data

            else:
                print(f"Error {response.status_code}: {response.text[:200]}")
                return None
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None

    def load(self, data: DataFrame, spark: SparkSession):
        from pyspark.sql.functions import to_date, col
        from concurrent.futures import ThreadPoolExecutor
        from pyspark.sql import Row
        import datetime

        print("\n" + "=" * 80)
        print("STARTING BATCH WEATHER ENRICHMENT")
        print("=" * 80)

        # Get distinct dates
        distinct_dates = sorted([row.date for row in data.select("date").distinct().collect()])
        total_dates = len(distinct_dates)
        print(f"\n FOUND {total_dates} DISTINCT DATES: {distinct_dates[:5]}...")

        # Prepare tracking structures
        weather_data = {}
        missing_dates = set(distinct_dates)

        # Generate batches of dates (max 30 days per batch)
        batches = []
        i = 0
        while i < len(distinct_dates):
            batch_dates = distinct_dates[i:i + 30]
            start = batch_dates[0].strftime("%Y-%m-%d")
            end = batch_dates[-1].strftime("%Y-%m-%d")
            batches.append((start, end, batch_dates))
            i += 30

        print(f"Created {len(batches)} date batches")

        # ======== STEP 1: Batch Processing with Retries ========
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for start, end, batch_dates in batches:
                # Submit batch with retry wrapper
                future = executor.submit(
                    self._fetch_batch_with_retry,
                    start, end, 40.7128, -74.0060, 3
                )
                futures.append((future, batch_dates))

            for future, batch_dates in futures:
                batch_result = future.result()
                for daily in batch_result:
                    if daily['date'] in missing_dates:
                        weather_data[daily['date']] = daily
                        missing_dates.remove(daily['date'])

        print(f"\nAfter batch processing, {len(missing_dates)} dates remain")

        # ======== STEP 2: Individual Date Retries ========
        if missing_dates:
            print("\nStarting individual date retries...")
            missing_list = sorted(missing_dates)
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for date in missing_list:
                    date_str = date.strftime("%Y-%m-%d")
                    future = executor.submit(
                        self._fetch_single_with_retry,
                        date_str, 40.7128, -74.0060, 3
                    )
                    futures.append((future, date))

                for future, date in futures:
                    daily_data = future.result()
                    if daily_data and daily_data.get('avg_temp') is not None:
                        weather_data[date] = daily_data
                        if date in missing_dates:
                            missing_dates.remove(date)

            print(f"After individual retries, {len(missing_dates)} dates failed")

        # ======== FINAL: Prepare Results ========
        weather_list = []
        for date in distinct_dates:
            if date in weather_data:
                weather_list.append(weather_data[date])
            else:
                weather_list.append({
                    "date": date,
                    "avg_temp": None,
                    "precipitation": None,
                    "weather_code": None,
                    "avg_humidity": None,
                    "avg_wind_speed": None,
                    "avg_cloud_cover": None,
                    "avg_snow_fall": None
                })

        # Create DataFrame
        weather_df = spark.createDataFrame(
            [Row(**item) for item in weather_list],
            schema=self.schema
        ).withColumn("date", to_date(col("date")))

        # Join with original data
        rawdata = data.join(weather_df, "date", "left")
        return rawdata

    def _fetch_batch_with_retry(self, start, end, lat, lon, max_retries):
        """Wrapper for batch fetch with retry logic"""
        for attempt in range(max_retries):
            result = self.fetch_weather_batch(start, end, lat, lon)
            if result:
                return result
            print(f"Batch {start}-{end} failed, attempt {attempt+1}/{max_retries}")
            time.sleep(2 ** attempt)  # Exponential backoff
        return []  # Return empty list after retries

    def _fetch_single_with_retry(self, date_str, lat, lon, max_retries):
        """Wrapper for single date fetch with retry logic"""
        for attempt in range(max_retries):
            result = self.fetch_weather_single(date_str, lat, lon)
            if result and result.get('avg_temp') is not None:
                return result
            print(f"Date {date_str} failed, attempt {attempt+1}/{max_retries}")
            time.sleep(2 ** attempt)  # Exponential backoff
        return None  # Return None after retries