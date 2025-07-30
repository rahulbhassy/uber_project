from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from Schema import sourceschema
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from DataCleaner import DataCleaner



setVEnv()
spark = create_spark_session()
'''
Fact Tables - uberfares , tripdetails
'''
sourceobject = "uberfares"
loadtype = "full"
sourceschema = sourceschema(sourcedefinition=sourceobject)

loadio = DataLakeIO(
    process='load',
    table=sourceobject,
    loadtype=loadtype
)
dataloader = DataLoader(
    path=loadio.filepath(),
    schema=sourceschema,
    filetype=loadio.file_ext(),
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
    table=sourceobject,
    state='current',
    loadtype=loadtype,
    runtype='dev'
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
