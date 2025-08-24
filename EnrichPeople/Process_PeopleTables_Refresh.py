from EnrichPeople.Config import config,layer , keys, updateitems
from Shared.sparkconfig import create_spark_session_large
from Shared.FileIO import SourceObjectAssignment , DataLakeIO , MergeIO
from Shared.DataWriter import DataWriter
from EnrichPeople.Harmonization import Harmonizer
from Shared.pyspark_env import setVEnv


setVEnv()
table = "customerprofile"
loadtype = 'full'
runtype = 'prod'
initial_load = 'yes'

spark = create_spark_session_large()
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
harmonizer = Harmonizer(
    table=table,
    loadtype=loadtype,
    runtype=runtype
)
currentio = DataLakeIO(
    process='write',
    table=table,
    state='current',
    layer=layer.get(table),
    loadtype=loadtype,
    runtype=runtype
)
destination_data = harmonizer.harmonize(
    spark=spark,
    dataframes=dataframes,
    currentio=currentio
)
if initial_load == 'yes':
    datawriter = DataWriter(
        loadtype=loadtype,
        path=currentio.filepath(),
        spark=spark,
        format='delta'
    )
    datawriter.WriteData(df=destination_data)
else:
    mergeconfig = MergeIO(
        table=table,
        currentio= currentio,
        updateitems=updateitems.get(table),
        key_columns= keys.get(table)
    )
    mergeconfig.merge(spark=spark,updated_df=destination_data)

spark.stop()
