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

readuberio = DataLakeIO(
    process='readenrich',
    sourceobject=sourceobjectuber,
    state='current'
)
uberpath = readuberio.filepath()
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW uber_trips AS
    SELECT *, 
        ST_Point(CAST(pickup_longitude AS Decimal(24,6)), 
                 CAST(pickup_latitude AS Decimal(24,6))) AS pickup_point
    FROM delta.`{uberpath}`
""")

readfeaturesio = DataLakeIO(
    process='readraw',
    sourceobject=sourceobjectborough,
    state='current'
)
dataloader = DataLoader(
    path=readfeaturesio.filepath(),
    filetype=readfeaturesio.filetype()
)
featuresdata = dataloader.LoadData(spark)
featuresdata.createOrReplaceTempView("boroughs")
spark.sql("""
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
currentio = DataLakeIO(
    process='enrichgeospatial',
    sourceobject='uber',
    state='current'
)
datawriter = DataWriter(
    mode='overwrite',
    path=currentio.filepath(),
    format=currentio.filetype()
)
datawriter.WriteData(df=spatial_analysis)
spark.stop()