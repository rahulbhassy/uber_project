from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import filetype,filepath
from Shared.DataWriter import DataWriter
from Harmonization import Distance

setEnv()
spark = create_spark_session()
sourcedefinition = "uberfares"


fileitem = filepath(
    process="enrichweather",
    sourceobject=sourcedefinition,
    state='current'
)
dataloader = DataLoader(
    path=fileitem,
    filetype='delta'
)
enriched_weather_data = dataloader.LoadData(spark)


enrichdistance = Distance()
enriched_data = enrichdistance.enrich(data=enriched_weather_data)
fileitem= filepath(
    process="enrich",
    sourceobject=sourcedefinition,
    state='current'
)
datawriter = DataWriter(
    mode='overwrite',
    path=fileitem
)
datawriter.WriteData(df=enriched_data)

spark.stop()