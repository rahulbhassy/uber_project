from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Schema import WeatherSchema
from Harmonization import WeatherAPI,PreHarmonizer
setEnv()
spark = create_spark_session()
sourcedefinition = "uberfares"
weatherschema = WeatherSchema()
loadtype = 'delta'

readio = DataLakeIO(
    process='read',
    table=sourcedefinition,
    state='current',
    loadtype='full',
    layer='raw'
)
dataloader = DataLoader(
    path=readio.filepath(),
    filetype='delta'
)
rawdata = dataloader.LoadData(spark)

currentio = DataLakeIO(
    process="enrichweather",
    table=sourcedefinition,
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
