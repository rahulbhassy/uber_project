from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from SourceWeather.Initialiser import Init
from SourceWeather.Schema import weather_schema
from SourceWeather.APILoader import WeatherAPI
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter


setVEnv()
loadtype = 'delta'
table = 'weatherdetails'
startdate = '2021-01-01'
enddate = '2024-12-31'
spark = create_spark_session()

init = Init(
    loadtype=loadtype,
    startdate=startdate,
    enddate=enddate
)
df = init.Load(spark=spark)

dataloader = WeatherAPI(schema=weather_schema)
weatherdetails = dataloader.load(
    data=df,
    spark=spark
)

currentio = DataLakeIO(
    process='write',
    table=table,
    state='current',
    layer='raw',
    loadtype=loadtype
)
writer = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    spark=spark
)
writer.WriteData(df=weatherdetails)
