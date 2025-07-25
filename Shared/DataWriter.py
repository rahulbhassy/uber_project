from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from connection import JDBC_URL, JDBC_PROPERTIES
from Shared.FileIO import IntermediateIO
from Shared.DataLoader import DataLoader


class DataWriter:
    """
    Unified DataWriter supporting configurable write format (default: delta) and JDBC.

    :param mode: write mode (e.g., 'overwrite', 'append')
    :param path: output path or JDBC table name
    :param format: output format ('delta', 'parquet', or 'jdbc')
    """

    def __init__(self, loadtype: str, path: str, spark: SparkSession, format: str = "delta"):
        self.loadtype = loadtype
        self.mode = 'overwrite'
        self.path = path
        self.format = format.lower()
        self.spark = spark

        if self.loadtype == "delta":
            self.mode = "append"

        print(f"DataWriter initialized: loadtype={loadtype}, mode={self.mode}, path={path}, format={self.format}")

    def WriteData(self, df: DataFrame):
        """
        Writes the DataFrame to the specified path or JDBC table in the chosen format.

        :param df: DataFrame to write
        """
        print(f"Starting write operation. Mode: {self.mode}, Format: {self.format}, Path: {self.path}")

        # JDBC write
        if self.format == 'jdbc':
            print(f"Writing to JDBC table: {self.path}")
            df.write.format('jdbc') \
                .option('url', JDBC_URL) \
                .option('dbtable', self.path) \
                .option('user', JDBC_PROPERTIES['user']) \
                .option('password', JDBC_PROPERTIES['password']) \
                .option('driver', JDBC_PROPERTIES['driver']) \
                .mode(self.mode) \
                .save()
            print("JDBC write completed successfully.")
        else:
            # File-based write (Delta, Parquet, CSV, etc.)
            print(f"Preparing file-based write using intermediate Parquet...")
            deltapath = self.WriteParquet(df=df)
            print(f"Loading intermediate Parquet from: {deltapath}")
            dataloader = DataLoader(
                path=deltapath,
                filetype='parquet'
            )
            df = dataloader.LoadData(self.spark)
            print(f"Writing final output to: {self.path} in {self.format} format")
            df.write.format(self.format).mode(self.mode).save(self.path)
            print("File write completed successfully.")

    def WriteParquet(self, df: DataFrame):
        deltaio = IntermediateIO(
            fullpath=self.path
        )
        deltapath = deltaio.get_deltapath()
        print(f"Writing intermediate Parquet to: {deltapath}")
        df.write.format('parquet').mode('overwrite').save(deltapath)
        return deltapath