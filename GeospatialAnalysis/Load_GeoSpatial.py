from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType
from pyspark.sql.functions import explode, col, to_json
from config.spark_config import create_spark_session_sedona
from GeospatialAnalysis.GeoJsonValidator import fix_geometry_pandas  # Ensure this is imported correctly
from GeospatialAnalysis.LoadTable import LoadTable
import os

# Set the PYSPARK_PYTHON environment variable
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\uber_project\.venv\Scripts\python.exe"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"
spark = create_spark_session_sedona()

def load_GeoSpatialData():
    # Define the schema for the GeoJSON file
    geojson_schema = StructType([
        StructField("type", StringType(), True),
        StructField("features", ArrayType(
            StructType([
                StructField("type", StringType(), True),
                StructField("properties", StructType([
                    StructField("name", StringType(), True),
                    StructField("cartodb_id", IntegerType(), True),
                    StructField("created_at", StringType(), True),  # Could be converted to TimestampType
                    StructField("updated_at", StringType(), True)  # Could be converted to TimestampType
                ]), True),
                StructField("geometry", StructType([
                    StructField("type", StringType(), True),
                    StructField("coordinates", ArrayType(
                        ArrayType(
                            ArrayType(
                                ArrayType(DoubleType())
                            )
                        )
                    ), True)
                ]), True)
            ])
        ), True)
    ])

    # Read the GeoJSON file
    boroughs_df = spark.read.option("multiline", "true") \
        .schema(geojson_schema) \
        .json("C:/Users/HP/uber_project/Data/Borough/fixed_new-york-city-boroughs.geojson")

    # Explode the features array and extract fields
    features_df = boroughs_df.select(explode("features").alias("feature"))
    features_df = features_df.select(
        col("feature.properties.name").alias("borough"),
        col("feature.geometry").alias("geometry")
    )
    features_df = features_df.withColumn("geometry_json", to_json("geometry"))

    # Apply the Pandas UDF to fix geometries
    fixed_features_df = features_df.withColumn("geometry_json", fix_geometry_pandas("geometry_json"))

    return fixed_features_df

