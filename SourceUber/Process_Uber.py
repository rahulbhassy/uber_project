from Shared.pyspark_env import setEnv
from Shared.sparkconfig import create_spark_session
from MetadataConfig import sourceschema,filepath,filetype
from DataLoader import DataLoader
from DataWriter import DataWriter
from DataCleaner import DataCleaner

setEnv()
spark = create_spark_session()
sourcedefinition = "uberfares"
sourceschema = sourceschema(sourcedefinition=sourcedefinition)

dataloader = DataLoader(
    path=filepath(process="load"),
    schema=sourceschema,
    filetype=filetype(sourcedefinition=sourcedefinition)
)
source_data = dataloader.LoadData(spark)

datacleaner = DataCleaner(
    sourcedefinition=sourcedefinition
)
destination_data = datacleaner.cleandata(sourcedata=source_data)

datawriter = DataWriter(
    mode="overwrite",
    path=filepath("write")
)
datawriter.WriteData(df=destination_data)
