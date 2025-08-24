from Shared.pyspark_env import setVEnv , stop_spark
from Shared.sparkconfig import create_spark_session
from Shared.FileIO import SourceObjectAssignment
from Shared.FileIO import DataLakeIO
from Balancing.config import CHECKS, SCHEMA , layer
from Balancing.utilities import Balancing
from Shared.DataWriter import DataWriter

setVEnv()
spark = create_spark_session()
runtype = 'prod'
loadtype = 'full'

final = spark.createDataFrame([], SCHEMA)
results = []
for table_name, info in CHECKS.items():
    sourcetable_assignment = SourceObjectAssignment(
        sourcetables=info['tables'],
        loadtype=loadtype,
        runtype=runtype
    )
    io_map = {}
    if layer.get(table_name) == 'raw':
        io_map = sourcetable_assignment.assign_DataLakeIO(layer=layer,process='load')
    else:
        io_map = sourcetable_assignment.assign_DataLakeIO(layer=layer)

    targetio = DataLakeIO(
        process='read',
        table=table_name,
        state='current',
        layer=layer.get(table_name),
        loadtype=loadtype,
        runtype=runtype
    )
    target_mapping = {table_name: targetio.filepath()}
    targetquery = info['targetquery'].format(**target_mapping)

    source_mapping = {t: io_map[t].filepath() for t in info['tables']}
    sourcequery = info['sourcequery'].format(**source_mapping)

    balancing = Balancing(
        table=table_name,
        sourcequery=sourcequery,
        targetquery=targetquery,
    )

    df,result = balancing.getResult(spark=spark)
    final = final.unionByName(df)
    results.append(result)

balancingio = DataLakeIO(
    process='write',
    table='balancingresults',
    state='current',
    layer='system',
    loadtype=loadtype,
    runtype=runtype
)

writer = DataWriter(
    loadtype='delta',
    path=balancingio.filepath(),
    format='delta',
    spark=spark
)

writer.WriteData(df=final)
if 'Fail' in results:
    raise Exception("Balancing checks failed. Please review the logs for details.")

stop_spark(spark=spark)


