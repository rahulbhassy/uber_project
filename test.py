
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

datareader = DataLoader(
    path=filepath(process="read"),
    filetype=filetype(sourcedefinition="destination")
)
df = datareader.LoadData(spark)
print(df.count())

