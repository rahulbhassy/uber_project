from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO , SourceObjectAssignment
from EnrichFare.Harmonization import Harmonizer
from EnrichFare.Config import config, layer

setVEnv()
spark = create_spark_session()
table = 'weatherimpact'
loadtype = 'full'
runtype = 'dev'

sourcetables = config[table]
sourceobjectassignments = SourceObjectAssignment(
    sourcetables=sourcetables,
    loadtype=loadtype,
    runtype=runtype
)
sourcereaders = sourceobjectassignments.assign_Readers(
    io_map=sourceobjectassignments.assign_DataLakeIO(layer=layer)
)
dataframes = sourceobjectassignments.getData(
    spark=spark,
    readers=sourcereaders
)

currentio = DataLakeIO(
    process='write',
    table=table,
    state='current',
    layer=layer.get(table),
    loadtype=loadtype,
    runtype=runtype
)
harmonizer = Harmonizer(
    table=table,
    loadtype=loadtype,
    runtype=runtype
)
destination_data = harmonizer.harmonize(
    spark=spark,
    dataframes=dataframes,
    currentio=currentio
)

datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    format='delta',
    spark=spark
)
datawriter.WriteData(df=destination_data)
