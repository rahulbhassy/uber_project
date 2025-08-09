from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from SourceWeather.Initialiser import Init
from SourceWeather.Schema import weather_schema
from SourceWeather.APILoader import WeatherAPI
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer


setVEnv()
table = 'fares'
spark = create_spark_session()
loadtype = 'full'

fileio = DataLakeIO(
    table=table,
    process='read',
    loadtype=loadtype,
    layer='enrich',
    state='current',
    runtype='prod'
)
reader = DataLoader(
    path=fileio.filepath(),
    filetype=fileio.file_ext(),
    loadtype=loadtype
)
df = reader.LoadData(spark=spark)

writer = DataWriter(
    loadtype=loadtype,
    path=fileio.filepath(),
    format='parquet'
)