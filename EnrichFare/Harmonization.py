from pyspark.sql import DataFrame, SparkSession
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader

class PreHarmonizer:
    def __init__(self,sourcedata: DataFrame,currentio: DataLakeIO, loadtype: str):
        self.sourcedata = sourcedata
        self.currentio = currentio
        self.loadtype = loadtype

    def preharmonize(self,spark: SparkSession):
        reader = DataLoader(
            path=self.currentio.filepath(),
            filetype=self.currentio.file_ext(),
            loadtype=self.loadtype
        )
        currentdata = reader.LoadData(spark=spark)
        self.sourcedata = self.sourcedata.join(
            currentdata,
            on=['trip_id']
        )
