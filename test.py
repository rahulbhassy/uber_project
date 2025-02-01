import os
from pyspark.sql import SparkSession

# Set the PYSPARK_PYTHON environment variable
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DeltaTest") \
    .config("spark.jars", r"C:\Spark\spark-3.4.4-bin-hadoop3\jars\delta-core_2.12-2.4.0.jar") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create and write Delta table
data = [(1, "test"), (2, "delta")]
df = spark.createDataFrame(data, ["id", "data"])
df.write.format("delta").save("C:/delta_table")

# Read Delta table
spark.read.format("delta").load("C:/delta_table").show()