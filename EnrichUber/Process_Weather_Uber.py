from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from EnrichUber.Schema import weather_schema
from Harmonization import PreHarmonizer,Harmonizer

setEnv()
spark = create_spark_session()
uber = "uberfares"
weather = "weatherdetails"
weatherschema = weather_schema
loadtype = 'full'


readio = DataLakeIO(
    process='read',
    table=uber,
    state='current',
    loadtype=loadtype,
    layer='raw'
)
reader = DataLoader(
    path=readio.filepath(),
    filetype='delta'
)
uberdata = reader.LoadData(spark)

readio = DataLakeIO(
    process='read',
    table=weather,
    state='current',
    loadtype=loadtype,
    layer='raw'
)
reader = DataLoader(
    path=readio.filepath(),
    filetype='delta'
)
weatherdata = reader.LoadData(spark)

currentio = DataLakeIO(
    process="enrichweather",
    table=uber,
    state='current',
    loadtype=loadtype,
    layer='enrich'
)

if loadtype == 'delta':
    preharmonizer = PreHarmonizer(
        currentio=currentio
    )
    uberdata = preharmonizer.preharmonize(sourcedata=uberdata, spark=spark)

harmonizer = Harmonizer(
    uberdata=uberdata,
    weatherdata=weatherdata,
    schema=weatherschema
)
enriched_weather_data = harmonizer.harmonize(spark=spark)


datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    spark=spark
)
datawriter.WriteData(df=enriched_weather_data)
spark.stop()
