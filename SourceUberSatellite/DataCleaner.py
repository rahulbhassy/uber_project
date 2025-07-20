from pyspark.sql.functions import col
from pyspark.sql import DataFrame, SparkSession
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO

class DataCleaner:

    # map each source to its primary key column
    _KEY_COLUMNS = {
        "driverdetails": "driver_id",
        "customerdetails": "customer_id",
        "vehicledetails": "vehicle_no",
    }

    def __init__(self, sourceobject: str,loadtype: str ,spark: SparkSession=None):
        self.sourceobject = sourceobject.lower()
        self.spark = spark
        self.loadtype = loadtype
        if self.sourceobject not in self._KEY_COLUMNS:
            raise ValueError(f"Unsupported sourceobject '{sourceobject}'")

    def _preharmonise(self,sourcedata: DataFrame):

        currentio=DataLakeIO(
            process="write",
            sourceobject=self.sourceobject,
            state='current'
        )
        dataloader = DataLoader(
            path=currentio.filepath(),
            filetype='delta'
        )
        key_col = self._KEY_COLUMNS[self.sourceobject]
        current_keys = dataloader.LoadData(self.spark).select(key_col)
        return sourcedata.join(current_keys, on=key_col, how="left_anti")

    def clean(self, data: DataFrame) -> DataFrame:
        key_col = self._KEY_COLUMNS[self.sourceobject]
        cleaned_data = data \
            .filter(col(key_col).isNotNull()) \
            .dropDuplicates([key_col])
        if self.loadtype == 'delta':
            return self._preharmonise(cleaned_data)
        return cleaned_data



