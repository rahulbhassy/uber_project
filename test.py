from sedona.spark import SedonaContext
from Shared.sparkconfig import create_spark_session_sedona
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer
from Shared.FileIO import DataLakeIO

setEnv()
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
df = df.filter(
    (df.pickup_borough.isNotNull()) |
    (df.dropoff_borough.isNotNull())
)
print("Without Nulls: ",df.count())