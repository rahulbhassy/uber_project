from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from SourceWeather.Initialiser import Init
from SourceWeather.Schema import weather_schema
from SourceWeather.APILoader import WeatherAPI
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.DataLoader import DataLoader


setVEnv()
table = 'weatherdetails'
spark = create_spark_session()
loadtype = 'full'
init = Init(
    loadtype=loadtype,
    spark=spark,
    table=table
)
df = init.Load()
currentio = DataLakeIO(
    process='write',
    table=table,
    state='current',
    layer='raw',
    loadtype=loadtype,
)
reader = DataLoader(
    path=currentio.filepath(),
    filetype=currentio.file_ext(),
    loadtype=loadtype
)
sourcedata = reader.LoadData(spark=spark)
df = df.join(
    sourcedata,
    on=['date'],
    how='left_anti'
)
print(df.count())