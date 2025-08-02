from sedona.spark import SedonaContext
from Shared.sparkconfig import create_spark_session_sedona
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO

setEnv()
spark = create_spark_session_sedona()
SedonaContext.create(spark)
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
        ST_Point(CAST(pickup_longitude AS Decimal(24,6)), 
                 CAST(pickup_latitude AS Decimal(24,6))) AS pickup_point,
        ST_Point(CAST(dropoff_longitude AS Decimal(24,6)), 
                 CAST(dropoff_latitude AS Decimal(24,6))) AS dropoff_point       
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
featuresdata = dataloader.LoadData(spark)
featuresdata.createOrReplaceTempView("boroughs")
spark.sql("""
        CREATE OR REPLACE TEMP VIEW boroughs_spatial AS
        SELECT borough, ST_GeomFromGeoJSON(geometry_json) AS geom
        FROM boroughs
    """)

# Perform spatial analysis
enriched_uber = spark.sql("""
    SELECT 
        u.*, 
        p.borough AS pickup_borough,
        d.borough AS dropoff_borough
    FROM uber_trips u
    LEFT JOIN boroughs_spatial p 
        ON ST_Within(u.pickup_point, p.geom)
    LEFT JOIN boroughs_spatial d 
        ON ST_Within(u.dropoff_point, d.geom)
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