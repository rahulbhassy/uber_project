from sedona.spark import SedonaContext
from Shared.sparkconfig import create_spark_session_sedona
from Shared.pyspark_env import setVEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer
from Shared.FileIO import DataLakeIO
from pyspark.sql.functions import date_format

setVEnv()
spark = create_spark_session_sedona()
reader = DataLakeIO(
    process='read',
    table='features',
    state='current',
    layer='raw',
    loadtype='full',
)

dataloader = DataLoader(
    path=reader.filepath(),
    filetype='delta',
)
df = dataloader.LoadData(spark=spark)

# Fix: Convert date/time columns to formatted strings
for col_name, col_type in df.dtypes:
    if col_type in ['date', 'timestamp']:
        df = df.withColumn(col_name, date_format(col_name, "yyyy-MM-dd HH:mm:ss"))

pandas_df = df.toPandas()
pandas_df.to_excel(
    "C:/Users/HP/uber_project/Data/Sandbox/DataSource/2025-08-01/uberfares.xlsx",
    sheet_name="Sheet1",
    index=False
)