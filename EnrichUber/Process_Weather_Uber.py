from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from EnrichUber.Schema import weather_schema
from Harmonization import WeatherAPI,PreHarmonizer

setEnv()
spark = create_spark_session()
uber = "uberfares"
weather = "weatherdetails"
weatherschema = weather_schema
loadtype = 'delta'



readio = DataLakeIO(
    process='read',
    table=uber,
    state='current',
    loadtype='full',
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
    loadtype='full',
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
    loadtype='full',
    layer='enrich'
)
preharmonizer = PreHarmonizer(
    currentio=currentio
)
enrichweather = WeatherAPI(schema=weatherschema)
enriched_weather_data = enrichweather.enrich(
    data=preharmonizer.preharmonize(sourcedata=rawdata,spark=spark),
    spark=spark
)
datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    spark=spark
)
datawriter.WriteData(df=enriched_weather_data)
spark.stop()
