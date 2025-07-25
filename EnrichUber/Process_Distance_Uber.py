from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Harmonization import Distance,PreHarmonizer

setEnv()
spark = create_spark_session()
sourcedefinition = "uberfares"
loadtype = 'full'

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
currentio = DataLakeIO(
    process="enrich",
    sourceobject=sourcedefinition,
    state='current'
)

if loadtype == 'delta':
    preharmonizer = PreHarmonizer(
        currentio=currentio
    )
    enriched_weather_data=preharmonizer.preharmonize(
        sourcedata=enriched_weather_data,
        spark=spark
    )


enrichdistance = Distance()
enriched_data = enrichdistance.enrich(data=enriched_weather_data)


datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    spark=spark
)
datawriter.WriteData(df=enriched_data)

spark.stop()