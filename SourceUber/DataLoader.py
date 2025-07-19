from pyspark.sql import SparkSession

class DataLoader:
    def __init__(self,path,filetype,schema=None):
        self.path = path
        self.schema = schema
        self.filetype = filetype

    def LoadData(
        self,
        spark: SparkSession
    ):
        if self.filetype == 'csv':
            return spark.read.csv(
                self.path,
                schema=self.schema,
                header=True
            )
        if self.filetype == "delta":
            return spark.read.format("delta").load(self.path)
