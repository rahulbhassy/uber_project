

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

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

                    # Diagnostic: Check extracted values
                    print(f"Extracted data for {target_date}: {extracted}")

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

        # Generate date ranges (max 30 days per batch)
        date_ranges = []
        for i in range(0, len(distinct_dates), 30):
            batch_dates = distinct_dates[i:i + 30]
            date_ranges.append((min(batch_dates), max(batch_dates)))

        print(f"Created {len(date_ranges)} date ranges")

        # Fetch data in parallel batches
        weather_data = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(
                self.fetch_weather_batch,
                start,
                end,
                40.7128, -74.0060  # NYC coordinates
            ) for start, end in date_ranges]

            for future in futures:
                batch_result = future.result()
                for daily in batch_result:
                    weather_data[daily["date"]] = daily

        print(f"Collected weather data for {len(weather_data)} dates")

        # Prepare final dataset
        missing_dates = []
        weather_list = []
        for date in distinct_dates:
            entry = weather_data.get(date)
            if entry:
                weather_list.append(entry)
            else:
                missing_dates.append(date)
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

        if missing_dates:
            print(f"MISSING DATA FOR {len(missing_dates)} DATES: {missing_dates[:5]}...")

        # Create DataFrame
        weather_df = spark.createDataFrame(
            [Row(**item) for item in weather_list],
            schema=self.schema
        ).withColumn("date", to_date(col("date")))


        # Join with original data
        rawdata = data.join(weather_df, "date", "left")
        return rawdata