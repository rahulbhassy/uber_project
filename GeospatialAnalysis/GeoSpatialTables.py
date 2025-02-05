from config.spark_config import create_spark_session_sedona
from GeospatialAnalysis.Load_GeoSpatial import load_GeoSpatialData
from sedona.spark import SedonaContext


# Create Spark session with Sedona
def GeoSpatialTable():
    spark = create_spark_session_sedona()

    # Register Sedona's UDFs and SQL functions
    SedonaContext.create(spark)

    # Load Uber trips data and create a temporary view
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW uber_trips AS
        SELECT *, 
            ST_Point(CAST(pickup_longitude AS Decimal(24,6)), 
                     CAST(pickup_latitude AS Decimal(24,6))) AS pickup_point
        FROM delta.`C:/Users/HP/uber_project/Data/Enriched/UberFares.csv`
    """)
    spark.sql("SELECT * FROM uber_trips LIMIT 10").show()

    # Load GeoSpatial data and create a temporary view
    load_GeoSpatialData().createOrReplaceTempView("boroughs")

    # Apply the spatial function
    boroughs_spatial_df = spark.sql("""
        CREATE OR REPLACE TEMP VIEW boroughs_spatial AS
        SELECT borough, ST_GeomFromGeoJSON(geometry_json) AS geom
        FROM boroughs
    """)

    # Perform spatial analysis
    spatial_analysis = spark.sql("""
        SELECT u.*, b.borough
        FROM uber_trips u
        JOIN boroughs_spatial b
        ON ST_Within(u.pickup_point, b.geom)
    """)
    # Save the DataFrame as a Delta table (overwrite mode)
    spatial_analysis.write.format("delta") \
        .mode("overwrite") \
        .save("C:/Users/HP/uber_project/Data/EnrichedGeoSpatial/spatial_analysis_delta")

    return spatial_analysis

GeoSpatialTable()
