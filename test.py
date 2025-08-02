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
    table='features',
    state='current',
    layer='raw',
    loadtype='full',
)

dataloader = DataLoader(
    path=reader.filepath(),
    filetype='delta',
)
df = dataloader.LoadData(spark=spark)
print(df.count())
df.show()
