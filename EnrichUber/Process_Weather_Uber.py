from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Schema import WeatherSchema
from Harmonization import WeatherAPI

setEnv()
spark = create_spark_session()
sourcedefinition = "uberfares"
weatherschema = WeatherSchema()


readio = DataLakeIO(
    process='readraw',
    sourceobject=sourcedefinition,
    state='current'
)
dataloader = DataLoader(
    path=readio.filepath(),
    filetype='delta'
)
rawdata = dataloader.LoadData(spark)

enrichweather = WeatherAPI(schema=weatherschema)
enriched_weather_data = enrichweather.enrich(
    data=rawdata,
    spark=spark
)
currentio = DataLakeIO(
    process="enrichweather",
    sourceobject=sourcedefinition,
    state='current'
)
datawriter = DataWriter(
    mode='overwrite',
    path=currentio.filepath()
)
datawriter.WriteData(df=enriched_weather_data)
spark.stop()
