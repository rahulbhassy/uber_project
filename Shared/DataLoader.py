from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from connection import JDBC_URL, JDBC_PROPERTIES

class DataLoader:
    """
    DataLoader for CSV, Delta, Parquet, and JDBC formats with optional schema support.

    filetype:
      - 'csv'
      - 'delta'
      - 'parquet'
      - 'jdbc'
    """
    def __init__(self, path: str, filetype: str, loadtype: str=None, schema: StructType = None):
        self.path = path
        self.schema = schema
        self.filetype = filetype.lower()
        self.loadtype= loadtype

    def LoadData(self, spark: SparkSession):
        """
        Loads data from the path depending on filetype.

        :param spark: SparkSession instance
        :return: DataFrame
        """
        # CSV
        if self.filetype == 'csv':
            reader = spark.read.option("header", True)
            if self.schema:
                reader = reader.schema(self.schema)
            return reader.csv(self.path)

        # Delta or Parquet
        if self.filetype in ('delta', 'parquet'):
            return spark.read.format(self.filetype).load(self.path)

        # JDBC
        if self.filetype == 'jdbc':
            return (
                spark.read
                    .format('jdbc')
                    .option('url', JDBC_URL)
                    .option('dbtable', self.path)
                    .option('user', JDBC_PROPERTIES['user'])
                    .option('password', JDBC_PROPERTIES['password'])
                    .option('driver', JDBC_PROPERTIES['driver'])
                    .load()
            )

        if self.filetype == 'geojson':
            reader = spark.read.option("multiline","true")
            if self.schema:
                reader = reader.schema(self.schema)
            return reader.json(self.path)

        raise ValueError(f"Unsupported filetype '{self.filetype}'")
