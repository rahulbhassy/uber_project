from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Harmonization import WeatherAPI,PreHarmonizer
import pandas
setEnv()
spark = create_spark_session()
sourcedefinition = "uberfares"
loadtype = 'delta'

readio = DataLakeIO(
    process='readraw',
    sourceobject=sourcedefinition,
    state='current'
)
dataloader = DataLoader(
    path=readio.filepath(),
    filetype='delta'
)
rawdata = dataloader.LoadData(spark)
pdf = rawdata.toPandas()