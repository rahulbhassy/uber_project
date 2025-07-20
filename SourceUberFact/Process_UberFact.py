from Shared.pyspark_env import setEnv
from Shared.sparkconfig import create_spark_session
from Schema import sourceschema
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from DataCleaner import DataCleaner

setEnv()
spark = create_spark_session()
'''
Fact Tables - uberfares , tripdetails
'''
sourceobject = "tripdetails"
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

datacleaner = DataCleaner(
    sourcedefinition=sourceobject,
    spark=spark,
    loadtype=loadtype
)
destination_data = datacleaner.cleandata(sourcedata=source_data)

currentio = DataLakeIO(
    process="write",
    sourceobject=sourceobject,
    state='current'
)
datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    format="delta",
    spark=spark
)
datawriter.WriteData(
    df=destination_data
)
spark.stop()
