from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from EnrichFare.Harmonization import Harmonizer

setVEnv()
spark = create_spark_session()
table = 'fares'
loadtype = 'full'
runtype = 'prod'

harmonizer = Harmonizer(
    table=table,
    loadtype=loadtype,
    runtype=runtype
)
destination_data = harmonizer.harmonize(spark=spark)
currentio = DataLakeIO(
    process='write',
    table=table,
    state='current',
    layer='enrich',
    loadtype=loadtype,
    runtype=runtype
)
datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    format='delta',
    spark=spark
)
datawriter.WriteData(df=destination_data)
