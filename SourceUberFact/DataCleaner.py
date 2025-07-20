
from pyspark.sql.functions import when, col, udf
from pyspark.sql.functions import to_timestamp, date_format
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import hour, dayofweek, month
from pyspark.sql import DataFrame, SparkSession
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO


# Geospatial validation UDF
def validate_coordinates(lat, lon):
    return (40.4 <= lat <= 40.9) and (-74.3 <= lon <= -73.7)

class DataCleaner:
    # map each source to its primary key column
    _KEY_COLUMNS = {
        "uberfares": "trip_id",
        "tripdetails": "trip_id"
    }
    def __init__(self,sourcedefinition,loadtype: str, spark: SparkSession = None ):
        self.sourcetype = sourcedefinition
        self.spark = spark
        self.loadtype = loadtype



    def _preharmonise(self,sourcedata: DataFrame):
        currentio=DataLakeIO(
            process="write",
            sourceobject=self.sourcetype,
            state='current'
        )
        dataloader = DataLoader(
            path=currentio.filepath(),
            filetype='delta'
        )
        key_col = self._KEY_COLUMNS[self.sourcetype]
        current_keys = dataloader.LoadData(self.spark).select(key_col)
        return sourcedata.join(current_keys, on=key_col, how="left_anti")

    def clean(
        self,
        rawdata: DataFrame
    ):
        coord_udf = udf(validate_coordinates, BooleanType())
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

        elif self.sourcetype == "tripdetails":
            return rawdata \
                .filter(col("trip_id").isNotNull()) \
                .filter(col("driver_id").isNotNull()) \
                .filter(col("customer_id").isNotNull()) \
                .dropDuplicates(["trip_id"])



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
        if self.sourcetype == 'uberfares':
            cleaned_data =  self.addtemporalfeatures(cleandata=cleaned_data)
        if self.loadtype == 'delta':
            return self._preharmonise(cleaned_data)
        return cleaned_data

