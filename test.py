from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer
from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setVEnv

setVEnv()
reader = DataLakeIO(
    process='read',
    table='weatherdetails',
    state='current',
    layer='raw',
    loadtype='full',
    runtype='dev'
)

dataloader = DataLoader(
    path=reader.filepath(),
    filetype='delta',
)
df = dataloader.LoadData(spark=create_spark_session())
df.show()