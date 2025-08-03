from sedona.spark import SedonaContext
from sedona.register.geo_registrator import SedonaRegistrator
from Shared.sparkconfig import create_spark_session_sedona
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO

setEnv()
spark = create_spark_session_sedona()
SedonaContext.create(spark)
SedonaRegistrator.registerAll(spark)
sourceobjectuber = 'uberfares'
sourceobjectborough = 'features'
loadtype = 'full'

readuberio = DataLakeIO(
    process='read',
    table=sourceobjectuber,
    state='current',
    loadtype=loadtype,
    layer='enrich'
)
uberpath = readuberio.filepath()
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW uber_trips AS
    SELECT *, 
        ST_Point(CAST(pickup_longitude AS DOUBLE), 
                 CAST(pickup_latitude AS DOUBLE)) AS pickup_point,
        ST_Point(CAST(dropoff_longitude AS DOUBLE), 
                 CAST(dropoff_latitude AS DOUBLE)) AS dropoff_point       
    FROM delta.`{uberpath}`
""")

readfeaturesio = DataLakeIO(
    process='read',
    table=sourceobjectborough,
    state='current',
    layer='raw',
    loadtype='full'
)
dataloader = DataLoader(
    path=readfeaturesio.filepath(),
    filetype=readfeaturesio.file_ext(),
    loadtype=loadtype
)

# 3. Your existing boroughs view with validation
featuresdata = dataloader.LoadData(spark)
featuresdata.createOrReplaceTempView("boroughs")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW boroughs_spatial AS
    SELECT borough, ST_GeomFromGeoJSON(geometry_json) AS geom
    FROM boroughs
    WHERE geometry_json IS NOT NULL
      AND ST_IsValid(ST_GeomFromGeoJSON(geometry_json))
""")

# 4. Cache for performance
spark.sql("CACHE TABLE boroughs_spatial")

# 5. Simple spatial join (no broadcast hints)
enriched_uber = spark.sql("""
    SELECT 
        u.*,
        p.borough AS pickup_borough,
        d.borough AS dropoff_borough
    FROM uber_trips u
    LEFT JOIN boroughs_spatial p
        ON ST_Contains(p.geom, u.pickup_point)
    LEFT JOIN boroughs_spatial d
        ON ST_Contains(d.geom, u.dropoff_point)
""")


# Save the DataFrame as a Delta table (overwrite mode)
currentio = DataLakeIO(
    process='write',
    table='uber',
    state='current',
    layer='enrich',
    loadtype='full'
)
datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    format=currentio.file_ext(),
    spark=spark
)
datawriter.WriteData(df=enriched_uber)
spark.stop()