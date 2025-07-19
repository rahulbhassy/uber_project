from Shared.pyspark_env import setEnv
from Shared.sparkconfig import create_spark_session
from Schema import sourceschema
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import filepath,filetype
from DataCleaner import DataCleaner

setEnv()
spark = create_spark_session()
sourceobject = "uberfares"
sourceschema = sourceschema(sourcedefinition=sourceobject)

dataloader = DataLoader(
    path=filepath(
        process="load",
        sourceobject=sourceobject
    ),
    schema=sourceschema,
    filetype=filetype(process='load',sourceobject=sourceobject)
)
source_data = dataloader.LoadData(spark)

datacleaner = DataCleaner(
    sourcedefinition=sourceobject
)
destination_data = datacleaner.cleandata(sourcedata=source_data)
fileitem = filepath(
    process="write",
    sourceobject=sourceobject,
    state="current"
)
datawriter = DataWriter(
    mode="overwrite",
    path=fileitem,
    format="delta"
)
datawriter.WriteData(
    df=destination_data
)
spark.stop()
