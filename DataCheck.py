from IPython.core.display_functions import display
from config.spark_config import create_spark_session
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

weather_schema = StructType([
    StructField("date", DateType(), nullable=False),
    StructField("avg_temp", DoubleType(), nullable=True),
    StructField("precipitation", DoubleType(), nullable=True)
])

spark = create_spark_session()
filepath = "C:/Users/HP/uber_project/Data/Enriched_Distance_uberData/uberData.csv"
uberData = spark.read.format("delta").load(filepath)
display(uberData.count())
filterData = uberData.filter(uberData.avg_temp.isNotNull() & uberData.precipitation.isNotNull())
filterData.show()
display(filterData.count())