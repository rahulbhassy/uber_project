from sedona.spark import SedonaContext
from Shared.sparkconfig import create_spark_session_sedona
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from pyspark.sql.functions import expr, col
from EnrichGeoSpatial.Harmonization import PreHarmonizer,Harmonizer

setEnv()
spark = create_spark_session_sedona()
SedonaContext.create(spark)
sourceobjectuber = 'uberfares'
sourceobjectborough = 'features'
sourceobjecttrip = 'tripdetails'
loadtype = 'full'

readuberio = DataLakeIO(
    process='read',
    table=sourceobjectuber,
    state='current',
    loadtype=loadtype,
    layer='enrich'
)
reader = DataLoader(
    path=readuberio.filepath(),
    filetype=readuberio.file_ext(),
    loadtype=loadtype
)
currentio = DataLakeIO(
    process='write',
    table='uber',
    state='current',
    layer='enrich',
    loadtype='full'
)
preharmonizer = PreHarmonizer(
    sourcedata=reader.LoadData(spark=spark),
    currentio=currentio,
    loadtype=loadtype
)
uber_df = preharmonizer.preharmonize(spark=spark)

# 3. Load boroughs data
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
featuresdata = dataloader.LoadData(spark=spark)

boroughs_df = featuresdata.withColumn(
    "geom",
    expr("ST_GeomFromGeoJSON(geometry_json)")
).select("borough", "geom")
boroughs_df.cache()


tripio = DataLakeIO(
    process='read',
    table=sourceobjecttrip,
    state='current',
    layer='raw',
    loadtype='full'
)
harmonizer = Harmonizer(
    sourcedata=uber_df,
    boroughs_df=boroughs_df,
    tripio=tripio
)
enriched_uber = harmonizer.harmonize(spark=spark)
# 7. Save results
datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    format=currentio.file_ext(),
    spark=spark
)
datawriter.WriteData(df=enriched_uber)
spark.stop()
