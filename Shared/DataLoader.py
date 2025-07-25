from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from connection import JDBC_URL, JDBC_PROPERTIES
import time


class DataLoader:
    """
    DataLoader for CSV, Delta, Parquet, JDBC, and GeoJSON formats with optional schema support.

    filetype options:
      - 'csv'
      - 'delta'
      - 'parquet'
      - 'jdbc'
      - 'geojson'
    """

    def __init__(self, path: str, filetype: str, loadtype: str = None, schema: StructType = None):
        self.path = path
        self.schema = schema
        self.filetype = filetype.lower()
        self.loadtype = loadtype

        print("\n" + "=" * 80)
        print("DATA LOADER INITIALIZED")
        print("=" * 80)
        print(f"📁 Path:       {self.path}")
        print(f"📝 Format:     {self.filetype.upper()}")
        print(f"🗂️ Schema:     {'Provided' if self.schema else 'Inferred'}")
        print(f"⚙️ Load Type:  {self.loadtype if self.loadtype else 'Default'}")
        print("=" * 80)

    def LoadData(self, spark: SparkSession):
        """
        Loads data from the path depending on filetype.

        :param spark: SparkSession instance
        :return: DataFrame
        """
        start_time = time.time()
        print(f"\n⏳ Loading {self.filetype.upper()} data from: {self.path}")

        try:
            # CSV
            if self.filetype == 'csv':
                print("🔄 Using CSV loader with options: header=True")
                reader = spark.read.option("header", True)
                if self.schema:
                    print("🔧 Applying custom schema")
                    reader = reader.schema(self.schema)
                df = reader.csv(self.path)

            # Delta or Parquet
            elif self.filetype in ('delta', 'parquet'):
                print(f"🔄 Using {self.filetype.upper()} loader")
                df = spark.read.format(self.filetype).load(self.path)

            # JDBC
            elif self.filetype == 'jdbc':
                print("🔄 Using JDBC loader")
                print(f"  🔗 URL: {JDBC_URL}")
                print(f"  🧑 User: {JDBC_PROPERTIES['user']}")
                print(f"  📋 Table: {self.path}")

                df = (
                    spark.read
                    .format('jdbc')
                    .option('url', JDBC_URL)
                    .option('dbtable', self.path)
                    .option('user', JDBC_PROPERTIES['user'])
                    .option('password', '******')  # Mask password
                    .option('driver', JDBC_PROPERTIES['driver'])
                    .load()
                )

            # GeoJSON
            elif self.filetype == 'geojson':
                print("🔄 Using GeoJSON loader with multiline=True")
                reader = spark.read.option("multiline", "true")
                if self.schema:
                    print("🔧 Applying custom schema")
                    reader = reader.schema(self.schema)
                df = reader.json(self.path)

            else:
                raise ValueError(f"Unsupported filetype '{self.filetype}'")

            # Post-load analysis
            load_time = time.time() - start_time
            print(f"\n✅ Successfully loaded data in {load_time:.2f} seconds")

            # Safe row count (avoid OOM for large datasets)
            try:
                row_count = df.count()
                print(f"🧮 Row Count:   {row_count:,}")
            except Exception as e:
                print(f"⚠️ Could not count rows: {str(e)[:100]}")

            col_count = len(df.columns)
            print(f"📦 Column Count: {col_count}")
            print("=" * 80)
            return df

        except Exception as e:
            load_time = time.time() - start_time
            print("\n" + "=" * 80)
            print(f"🚨 ERROR LOADING DATA (after {load_time:.2f}s)")
            print("=" * 80)
            print(f"Error Type:    {type(e).__name__}")
            print(f"Error Message: {str(e)[:500]}")
            print("=" * 80)
            raise  # Re-raise exception after logging