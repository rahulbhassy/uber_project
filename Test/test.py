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
full = fares.filter(
    (fares.pickupboroughsource == 'features') &
    (fares.dropoffboroughsource == 'features')
)
half1 = fares.filter(
    (fares.pickupboroughsource == 'features') &
    (fares.dropoffboroughsource != 'features')
)
half2 = fares.filter(
    (fares.pickupboroughsource != 'features') &
    (fares.dropoffboroughsource == 'features')
)
empty = fares.filter(
    (fares.pickupboroughsource != 'features') &
    (fares.dropoffboroughsource != 'features')
)
print(f"pickup is features {half1.count()}")
print(f"dropoff is features {half2.count()}")
print(f"both are features {full.count()}")
print(f"neither are features {empty.count()}")
viewer = SparkTableViewer(df=fares)
viewer.display()
