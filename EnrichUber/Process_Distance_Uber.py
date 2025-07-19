from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Harmonization import Distance

setEnv()
spark = create_spark_session()
sourcedefinition = "uberfares"

readio = DataLakeIO(
    process="enrichweather",
    sourceobject=sourcedefinition,
    state='current'
)
dataloader = DataLoader(
    path=readio.filepath(),
    filetype='delta'
)
enriched_weather_data = dataloader.LoadData(spark)


enrichdistance = Distance()
enriched_data = enrichdistance.enrich(data=enriched_weather_data)
currentio = DataLakeIO(
    process="enrich",
    sourceobject=sourcedefinition,
    state='current'
)
datawriter = DataWriter(
    mode='overwrite',
    path=currentio.filepath()
)
datawriter.WriteData(df=enriched_data)

spark.stop()