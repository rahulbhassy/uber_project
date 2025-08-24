from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from Balancing.config import SCHEMA

class Balancing:
    def __init__(self,table: str,sourcequery: str , targetquery: str):

        self.table = table
        self.sourcequery = sourcequery
        self.targetquery = targetquery

    def executeQuery(self, spark:SparkSession , query: str):
        """
        Execute a SQL query and return the result as a DataFrame.
        """
        val = spark.sql(query).collect()[0]
        return val[0]

    def getResult(self, spark: SparkSession):
        """
        Get the result of the balancing check.
        """
        rows = []
        source_count = self.executeQuery(spark, self.sourcequery)
        target_count = self.executeQuery(spark, self.targetquery)

        difference = source_count - target_count
        result = "Pass" if difference == 0 else "Fail"

        rows.append(Row(
            table_name=str(self.table),
            expected_count=int(source_count),
            actual_count=int(target_count),
            difference=int(difference),
            result=result,
            date=datetime.now()
        ))
        return spark.createDataFrame(rows, schema=SCHEMA) ,result


