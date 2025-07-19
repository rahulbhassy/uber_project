
from pyspark.sql.functions import when, col, udf
from pyspark.sql.functions import to_timestamp, date_format
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import hour, dayofweek, month
from pyspark.sql import DataFrame

class DataCleaner:
    def __init__(self,sourcedefinition):
        self.sourcetype = sourcedefinition

    # Geospatial validation UDF
    def validate_coordinates(self,lat, lon):
        return (40.4 <= lat <= 40.9) and (-74.3 <= lon <= -73.7)

    def clean(
        self,
        rawdata: DataFrame
    ):
        coord_udf = udf(self.validate_coordinates, BooleanType())
        if self.sourcetype == "uberfares":
            return  rawdata \
                .filter(
                "pickup_latitude IS NOT NULL AND pickup_longitude IS NOT NULL AND dropoff_latitude IS NOT NULL AND dropoff_longitude IS NOT NULL") \
                .filter(coord_udf(col("pickup_latitude"), col("pickup_longitude"))) \
                .withColumn("passenger_count",
                            when(col("passenger_count").isNull(), 1)
                            .otherwise(col("passenger_count").cast("integer"))) \
                .dropDuplicates(["trip_id"]) \
                .filter(col("fare_amount") > 2.5) \
                .withColumn("date", date_format("pickup_datetime", "yyyy-MM-dd"))

    def addtemporalfeatures(
        self,
        cleandata: DataFrame
    ):
        if self.sourcetype == "uberfares":
            return cleandata \
                .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
                .withColumn("pickup_day", dayofweek(col("pickup_datetime"))) \
                .withColumn("pickup_month", month(col("pickup_datetime"))) \
                .filter(col("date").isNotNull())

    def cleandata(
        self,
        sourcedata: DataFrame
    ):
        cleaned_data = self.clean(rawdata=sourcedata)
        return self.addtemporalfeatures(cleandata=cleaned_data)

