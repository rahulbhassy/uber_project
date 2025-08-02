from pyspark.sql.types import (
    StructType, StructField,
    DateType, DoubleType, IntegerType
)

weather_schema = StructType([
    StructField("date", DateType(), True),
    StructField("avg_temp", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("avg_humidity", DoubleType(), True),
    StructField("avg_wind_speed", DoubleType(), True),
    StructField("avg_cloud_cover", DoubleType(), True),
    StructField("avg_snow_fall", DoubleType(), True),
])

