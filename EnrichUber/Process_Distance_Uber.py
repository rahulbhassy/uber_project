from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setVEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Harmonization import Distance,PreHarmonizer

setVEnv()
spark = create_spark_session()
table = "uberfares"
loadtype = 'full'

readio = DataLakeIO(
    process="enrichweather",
    table=table,
    state='current',
    layer='enrich',
    loadtype=loadtype
)
dataloader = DataLoader(
    path=readio.filepath(),
    filetype='delta'
)
enriched_weather_data = dataloader.LoadData(spark)
currentio = DataLakeIO(
    process="write",
    table=table,
    state='current',
    layer='enrich',
    loadtype=loadtype
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