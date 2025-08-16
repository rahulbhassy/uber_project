from functools import reduce

from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from SourceWeather.Initialiser import Init
from SourceWeather.Schema import weather_schema
from SourceWeather.APILoader import WeatherAPI
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer
from pyspark.sql.functions import avg, col, lit , round


setVEnv()
table = 'fares'
spark = create_spark_session()
loadtype = 'full'

fileio = DataLakeIO(
    process="read",
    table=table,
    state='current',
    loadtype=loadtype,
    layer='enrich'
)
reader = DataLoader(
    path=fileio.filepath(),
    filetype=fileio.file_ext(),
    loadtype=loadtype
)

fares = reader.LoadData(spark=spark)

viewer = SparkTableViewer(df=fares)
viewer.display()
