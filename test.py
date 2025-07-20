from Shared.pyspark_env import setEnv
from Shared.sparkconfig import create_spark_session
from SourceUberSatellite.Schema import sourceschema
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from DataCleaner import DataCleaner

setEnv()
spark = create_spark_session()
'''
Fact Tables - uberfares , tripdetails
'''
sourceobject = "customerdetails"
loadtype = "full"
sourceschema = sourceschema(sourcedefinition=sourceobject)

loadio = DataLakeIO(
    process='load',
    sourceobject=sourceobject,
    loadtype=loadtype
)
dataloader = DataLoader(
    path=loadio.filepath(),
    schema=sourceschema,
    filetype=loadio.filetype(),
    loadtype=loadtype
)
source_data = dataloader.LoadData(spark)
print(source_data.count())

df = spark.read.format("delta").load("C:/Users/HP/uber_project/Data/Raw/customerdetails/current/customerdetails.delta")
print(df.count())