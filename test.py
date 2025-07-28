from sedona.spark import SedonaContext
from Shared.sparkconfig import create_spark_session_sedona
from Shared.pyspark_env import setEnv


setEnv()
spark = create_spark_session_sedona()
SedonaContext.create(spark)
from Shared.FileIO import DataLakeIO, SparkTableViewer
setEnv()
path = 'C:/Users/HP/uber_project/Data/Enrich/Enriched/uberfares/current/uberfares.delta'
rawdata = spark.read.format('delta').load(path)
viewer = SparkTableViewer(df=rawdata,table_name='spatial')
viewer.display()

