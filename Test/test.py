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
    table=table,
    process='read',
    loadtype=loadtype,
    layer='enrich',
    state='current',
    runtype='dev'
)
reader = DataLoader(
    path=fileio.filepath(),
    filetype=fileio.file_ext(),
    loadtype=loadtype
)
df = reader.LoadData(spark=spark)
df = df.filter(
    df.is_weather_extreme == False
).groupBy('pickup_borough','dropoff_borough').agg(
    round(avg('fare_amount'),2).alias('avg_fare_amount'),
    round(avg('trip_duration_min'),2).alias('avg_trip_duration_min')
)

viewer = SparkTableViewer(df=df)
viewer.display()