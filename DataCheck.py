
from config.spark_config import create_spark_session_sedona
from sedona.spark import SedonaContext
from pyspark.sql.functions import col
spark = create_spark_session_sedona()
SedonaContext.create(spark)
filepath = "C:/Users/HP/uber_project/Data/EnrichedGeoSpatial/spatial_analysis_delta"
uberData = spark.read.format("delta").load(filepath)
uberData = uberData.drop(col("pickup_point"))
uberData.write \
        .mode("overwrite") \
        .parquet("C:/Users/HP/uber_project/Data/Enriched_Without_GeoSpatial/UberFares.parquet")
uberData.printSchema()