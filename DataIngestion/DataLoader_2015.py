from IPython.core.display_functions import display

from config.spark_config import create_spark_session

spark = create_spark_session()
UberData = spark.read.csv('C:/Users/HP/uber_project/Data/2015/uber-raw-*.csv',header=True,inferSchema=True)
UberData.show()
UberData.describe().show()