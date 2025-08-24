from EnrichPeople.Config import config,layer
from Shared.sparkconfig import create_spark_session
from Shared.FileIO import SourceObjectAssignment
from Shared.DataWriter import DataWriter
from SourceUberSatellite.Process_UberSatellite import loadtype
from Shared.pyspark_env import setVEnv

setVEnv()
table = "customerprofile"
loadtype = 'full'
runtype = 'prod'

spark = create_spark_session()
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
