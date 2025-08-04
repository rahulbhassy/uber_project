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

viewer = SparkTableViewer(df=df)
viewer.display()