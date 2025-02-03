
from config.spark_config import create_spark_session

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

weather_schema = StructType([
    StructField("date", DateType(), nullable=False),
    StructField("avg_temp", DoubleType(), nullable=True),
    StructField("precipitation", DoubleType(), nullable=True)
])

spark = create_spark_session()
filepath = "C:/Users/HP/uber_project/Data/Cleaned_UberFares/UberFares.csv"
uberData = spark.read.format("delta").load(filepath)
dates = uberData.select(uberData.date.alias("date")).distinct().collect()

print(len(dates))