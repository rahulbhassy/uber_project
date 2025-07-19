from Shared.pyspark_env import setEnv
from Shared.sparkconfig import create_spark_session
from Schema import sourceschema
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from DataCleaner import DataCleaner

setEnv()
spark = create_spark_session()
sourceobject = "uberfares"
sourceschema = sourceschema(sourcedefinition=sourceobject)

loadio = DataLakeIO(
    process='load',
    sourceobject=sourceobject
)
dataloader = DataLoader(
    path=loadio.filepath(),
    schema=sourceschema,
    filetype=loadio.filetype()
)
source_data = dataloader.LoadData(spark)

datacleaner = DataCleaner(
    sourcedefinition=sourceobject
)
destination_data = datacleaner.cleandata(sourcedata=source_data)
currentio = DataLakeIO(
    process="write",
    sourceobject=sourceobject,
    state='current'
)
datawriter = DataWriter(
    mode="overwrite",
    path=currentio.filepath(),
    format="delta"
)
datawriter.WriteData(
    df=destination_data
)
spark.stop()
