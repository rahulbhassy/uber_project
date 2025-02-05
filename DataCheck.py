
from config.spark_config import create_spark_session_sedona
from sedona.spark import SedonaContext

spark = create_spark_session_sedona()
SedonaContext.create(spark)
filepath = "C:/Users/HP/uber_project/Data/EnrichedGeoSpatial/spatial_analysis_delta"
uberData = spark.read.format("delta").load(filepath)
uberData.show()