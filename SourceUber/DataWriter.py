from pyspark.sql import DataFrame

class DataWriter:
    def __init__(self,mode,path):
        self.mode = mode
        self.path = path

    def WriteData(
        self,
        df: DataFrame
    ):
        df.write.format("delta").mode(self.mode).save(self.path)
