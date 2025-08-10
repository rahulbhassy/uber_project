from typing import List
from pyspark.sql import *
from pyspark.sql.functions import create_map, lit, coalesce , when ,round
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql import DataFrame, SparkSession
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader

class PreHarmonizer:
    def __init__(self,sourcedata: DataFrame,currentio: DataLakeIO, loadtype: str):
        self.sourcedata = sourcedata
        self.currentio = currentio
        self.loadtype = loadtype

    def preharmonize(self,spark: SparkSession,keycolumn: List[str]):
        reader = DataLoader(
            path=self.currentio.filepath(),
            filetype=self.currentio.file_ext(),
            loadtype=self.loadtype
        )
        currentdata = reader.LoadData(spark=spark)
        self.sourcedata = self.sourcedata.join(
            currentdata,
            on=keycolumn,
            how='left_anti'
        )
        return self.sourcedata

class FareHarmonizer:
    _LAYER = {
        "uber" : "enrich",
        "tripdetails" : "raw",
        "fares" : "enrich"
    }
    _month_map = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    _day_map = {
        1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
        5: "Thursday", 6: "Friday", 7: "Saturday"
    }

    rain_bins = [0.1, 2, 10]  # mm
    rain_labels = ["No Rain", "Slight Rain", "Medium Rain", "Heavy Rain"]

    snow_bins = [0.1, 1, 5]  # mm
    snow_labels = ["No Snow", "Slight Snow", "Medium Snow", "Heavy Snow"]

    wind_bins = [5, 20, 40]  # km/h
    wind_labels = ["Calm", "Slight Wind", "Windy", "Heavy Wind"]
    temp_bins = [0, 5, 15, 25, 35, 40]  # boundaries
    temp_labels = [
        "Freezing",  # <= 0°C
        "Very Cold",  # 0 < t <= 5
        "Cool",  # 5 < t <= 15
        "Mild",  # 15 < t <= 25
        "Warm",  # 25 < t <= 35
        "Hot",  # 35 < t <= 40
        "Extreme Heat"  # > 40
    ]

    def __init__(self,loadtype: str,runtype: str = 'full'):
        self.loadtype = loadtype
        self.runtype = runtype
        self.uberio = DataLakeIO(
            process='read',
            table='uber',
            loadtype=self.loadtype,
            state='current',
            layer=self._LAYER.get('uber'),
            runtype=self.runtype
        )
        self.tripio = DataLakeIO(
            process='read',
            table='tripdetails',
            loadtype=self.loadtype,
            state='current',
            layer=self._LAYER.get('tripdetails'),
            runtype=self.runtype
        )
        self.currentio = DataLakeIO(
            process='read',
            table='fares',
            loadtype=self.loadtype,
            state='current',
            layer=self._LAYER.get('fares'),
            runtype=self.runtype
        )
        self.mapping_monthexpr = create_map([lit(x) for pair in self._month_map.items() for x in pair])
        self.mapping_dayexpr = create_map([lit(x) for pair in self._day_map.items() for x in pair])


    def categorize_metric(self,col_name, bins, labels, null_label="Unknown"):
        """
        bins: list of numeric boundaries (ascending). e.g. [0.1, 2, 10]
        labels: list of label names, len(labels) == len(bins)+1
        produces a column expression with chained WHENs
        """
        expr = when(col(col_name).isNull(),lit(null_label))
        # first bucket: <= bins[0]
        expr = expr.when(col(col_name) <= bins[0], lit(labels[0]))
        # middle buckets
        for i in range(1, len(bins)):
            expr = expr.when((col(col_name) > bins[i - 1]) & (col(col_name) <= bins[i]),lit(labels[i]))
        # final otherwise
        expr = expr.otherwise(lit(labels[-1]))
        return expr

    def harmonize(self, spark: SparkSession) -> DataFrame:
        # Implement fare harmonization logic here
        uberreader = DataLoader(
            path=self.uberio.filepath(),
            filetype='delta',
            loadtype=self.loadtype
        )
        tripreader = DataLoader(
            path=self.tripio.filepath(),
            filetype='delta',
            loadtype=self.loadtype
        )
        uberdf  = uberreader.LoadData(spark=spark)
        tripdf = tripreader.LoadData(spark=spark)
        # Pre-harmonization step
        if self.loadtype == 'delta':
            uberph = PreHarmonizer(
                sourcedata=uberdf,
                currentio=self.currentio,
                loadtype=self.loadtype
            )
            tripph = PreHarmonizer(
                sourcedata=tripdf,
                currentio=self.currentio,
                loadtype=self.loadtype
            )
            uberdf = uberph.preharmonize(spark=spark,keycolumn=['trip_id'])
            tripdf = tripph.preharmonize(spark=spark,keycolumn=['trip_id'])

        destinationdata = (
            uberdf.alias('u').join(
                tripdf.alias('t'),
                on='trip_id',
                how='inner'
            ).withColumn(
                "trip_duration_min",
                (unix_timestamp(col("u.dropoff_datetime")) - unix_timestamp(col("u.pickup_datetime"))) / 60.0
            ).withColumn(
                "fare_per_km",
                when(
                    col("distance_km") > 0,
                    col("fare_amount") / col("distance_km")
                ).otherwise(None)
            ).withColumn(
                "fare_per_min",
                when(
                    col("trip_duration_min") > 0,
                    col("fare_amount") / col("trip_duration_min")
                ).otherwise(None)
            ).withColumn(
                "tip_pct",
                when(
                    (col("fare_amount") + col("tip_amount")) == 0, None
                ).otherwise((col("tip_amount") / (col("fare_amount") + 1e-9))*100)
            ).withColumn(
                "pickup_month",
                self.mapping_monthexpr.getItem(col("pickup_month"))
            ).withColumn(
                "pickup_day",
                self.mapping_dayexpr.getItem(col("pickup_day"))
            ).withColumn(
        "pickup_period",
                 when(col("pickup_hour").isNull(), lit("Unknown"))
                .when((col("pickup_hour") >= 1) & (col("pickup_hour") <= 3), lit("After Midnight"))
                .when((col("pickup_hour") > 3) & (col("pickup_hour") <= 5), lit("Early Morning"))
                .when((col("pickup_hour") > 5) & (col("pickup_hour") <= 9), lit("Morning Rush"))
                .when((col("pickup_hour") > 9) & (col("pickup_hour") <= 15), lit("Midday"))
                .when((col("pickup_hour") > 15) & (col("pickup_hour") <= 19), lit("Evening Rush"))
                .when((col("pickup_hour") > 19) & (col("pickup_hour") <= 21), lit("Night"))
                .when((col("pickup_hour") > 21) & (col("pickup_hour") <= 24), lit("Late Night"))
                .otherwise(lit("InvalidHour"))  # covers 0, 25, negative, etc.
            ).withColumn(
                "total_fareamount",
                col("u.fare_amount") + col("t.tip_amount")
            ).withColumn(
                "rain_intensity",
                self.categorize_metric("precipitation", self.rain_bins, self.rain_labels)
            ).withColumn(
                "snow_intensity",
                self.categorize_metric("snow_fall", self.snow_bins, self.snow_labels)
            ).withColumn(
                "wind_intensity",
                self.categorize_metric("wind_speed", self.wind_bins, self.wind_labels)
            ).withColumn(
                "temperature_intensity",
                self.categorize_metric("temperature", self.temp_bins, self.temp_labels)
            ).select(
                col("u.trip_id"),
                col("u.date"),
                col("u.pickup_datetime"),
                col("u.dropoff_datetime"),
                col("u.distance_km"),
                "trip_duration_min",
                col("u.fare_amount"),
                col("t.tip_amount"),
                round(col("fare_per_km"), 2).alias("fare_per_km"),
                round(col("fare_per_min"), 2).alias("fare_per_min"),
                round(col("tip_pct"), 2).alias("tip_pct"),
                round(col("total_fareamount"), 2).alias("total_fareamount"),
                "pickup_month",
                "pickup_day",
                "pickup_period",
                "rain_intensity",
                "snow_intensity",
                "wind_intensity",
                "temperature_intensity",
                col("u.pickup_borough"),
                col("u.dropoff_borough"),
                col("u.pickupboroughsource"),
                col("u.dropoffboroughsource")
            )
        )
        return destinationdata

class Harmonizer:
    _harmonizer_map = {
        "fares": FareHarmonizer
    }

    def __init__(self,table,loadtype: str,runtype: str = 'full'):
        self.table = table
        self.loadtype = loadtype
        self.runtype = runtype
        self.harmonizer_class = self._harmonizer_map.get(table)

        if not self.harmonizer_class:
            raise ValueError(f"No harmonizer found for source: {table}")
        self.harmonizer_instance = self.harmonizer_class(
            loadtype=self.loadtype,
            runtype=self.runtype
        )

    def harmonize(self,spark: SparkSession) -> DataFrame:
        """Instance method to harmonize data using the selected harmonizer"""
        return self.harmonizer_instance.harmonize(spark=spark)


