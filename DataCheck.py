
from config.spark_config import create_spark_session

spark = create_spark_session()
UberData = spark.read.csv('C:/Users/HP/uber_project/Data/UberFaresData/uber.csv',inferSchema=True,header=True)
UberData.show()