from pyspark.sql import DataFrame
from connection import JDBC_URL, JDBC_PROPERTIES

class DataWriter:
    """
    Unified DataWriter supporting configurable write format (default: delta) and JDBC.

    :param mode: write mode (e.g., 'overwrite', 'append')
    :param path: output path or JDBC table name
    :param format: output format ('delta', 'parquet', or 'jdbc')
    """
    def __init__(self, mode: str, path: str, format: str = "delta"):
        self.mode = mode
        self.path = path
        self.format = format.lower()

    def WriteData(self, df: DataFrame):
        """
        Writes the DataFrame to the specified path or JDBC table in the chosen format.

        :param df: DataFrame to write
        """
        # JDBC write
        if self.format == 'jdbc':
            df.write.format('jdbc') \
                .option('url', JDBC_URL) \
                .option('dbtable', self.path) \
                .option('user', JDBC_PROPERTIES['user']) \
                .option('password', JDBC_PROPERTIES['password']) \
                .option('driver', JDBC_PROPERTIES['driver']) \
                .mode(self.mode) \
                .save()
        else:
            # File-based write (Delta, Parquet, CSV, etc.)
            df.write.format(self.format).mode(self.mode).save(self.path)
