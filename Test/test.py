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
table = 'weatherdetails'
spark = create_spark_session()
loadtype = 'full'

fileio = DataLakeIO(
    process="read",
    table=table,
    state='current',
    loadtype=loadtype,
    layer='raw'
)
reader = DataLoader(
    path=fileio.filepath(),
    filetype=fileio.file_ext(),
    loadtype=loadtype
)
missw = reader.LoadData(spark=spark)

fileio = DataLakeIO(
    process="enrichweather",
    table='uberfares',
    state='current',
    loadtype=loadtype,
    layer='enrich'
)
reader = DataLoader(
    path=fileio.filepath(),
    filetype=fileio.file_ext(),
    loadtype=loadtype
)
fullw = reader.LoadData(spark=spark)
'''
df = df.filter(
    df.is_weather_extreme == False
).groupBy('pickup_period','pickup_borough','dropoff_borough').agg(
    round(avg('fare_amount'),2).alias('avg_fare_amount'),
    round(avg('trip_duration_min'),2).alias('avg_trip_duration_min')
)
'''
other_cols = [c for c in missw.columns if c != 'date']
missw_nulls = missw.filter(
    reduce(lambda a, b: a & b, [col(c).isNull() for c in other_cols])
)
misswnon_nulls = missw.filter(
    ~reduce(lambda a, b: a & b, [col(c).isNull() for c in other_cols])
)
'''
fullw = fullw.groupBy(
    'date'
).agg(
    avg('avg_temp').alias('avg_temp'),
    avg('precipitation').alias('precipitation'),
    avg('weather_code').alias('weather_code'),
    avg('avg_humidity').alias('avg_humidity'),
    avg('avg_wind_speed').alias('avg_wind_speed'),
    avg('avg_cloud_cover').alias('avg_cloud_cover'),
    avg('avg_snow_fall').alias('avg_snow_fall')
).select(
    'date',
    *other_cols
)
fullw = fullw.filter(
    fullw.date == '2012-10-18'
)
missw_nulls = missw_nulls.alias('m').join(
    fullw.alias('f'),
    on='date',
    how='left'
).select(
    'm.date',
    *[col('f.' + c) for c in other_cols]
)

last = missw_nulls.filter(
    reduce(lambda a, b: a & b, [col(c).isNull() for c in other_cols])
)
'''

print(missw_nulls.count())
viewer = SparkTableViewer(df=missw_nulls)
viewer.display()
