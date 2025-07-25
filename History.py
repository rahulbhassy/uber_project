from Shared.sparkconfig import create_spark_session
from Shared.FileIO import DeltaLakeOps

spark = create_spark_session()
path = 'C:/Users/HP/uber_project/Data/Raw/uberfares/current/uberfares.delta'

ops = DeltaLakeOps(spark=spark, path=path)
ops.getHistory(count=True)

