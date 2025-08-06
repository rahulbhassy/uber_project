from sedona.spark import SedonaContext
from Shared.sparkconfig import create_spark_session_sedona
from Shared.pyspark_env import setVEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer
from Shared.FileIO import DataLakeIO
from pyspark.sql.functions import date_format

setVEnv()
spark = create_spark_session_sedona()
SedonaContext.create(spark)
reader = DataLakeIO(
    process='read',
    table='uber',
    state='current',
    layer='enrich',
    loadtype='full',
)

dataloader = DataLoader(
    path=reader.filepath(),
    filetype='delta',
)
df = dataloader.LoadData(spark=spark)

count1 = df.filter(
    (df.pickupboroughsource == 'tripdetails') &
    (df.dropoffboroughsource == 'tripdetails')
).count()
count2 = df.filter(
    (df.pickupboroughsource == 'features') &
    (df.dropoffboroughsource == 'tripdetails')
).count()
count3 = df.filter(
    (df.pickupboroughsource == 'tripdetails') &
    (df.dropoffboroughsource == 'features')
).count()
count4 = df.filter(
    (df.pickupboroughsource == 'features') &
    (df.dropoffboroughsource == 'features')
).count()
print(f"Count of records with both boroughs from tripdetails: {count1}")
print(f"Count of records with pickup borough from features and dropoff from tripdetails: {count2}")
print(f"Count of records with pickup borough from tripdetails and dropoff from features: {count3}")
print(f"Count of records with both boroughs from features: {count4}")

df = df.groupBy('pickup_borough','dropoff_borough','pickupboroughsource','dropoffboroughsource').count()
viewer = SparkTableViewer(df=df)
viewer.display()