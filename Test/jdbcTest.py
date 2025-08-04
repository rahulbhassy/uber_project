from pyspark.sql import SparkSession

# Create the Spark session with proper file URI prefixes
spark = SparkSession.builder \
    .appName("UberETL") \
    .config("spark.jars",
        "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/delta-core_2.12-2.4.0.jar,"
        "file:///C:/Users/HP/uber_project/config/mysql-connector-j-8.0.33.jar"
    ) \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:///C:/Users/HP/uber_project/config/log4j.properties") \
    .getOrCreate()

# Define JDBC connection properties for MySQL
jdbc_url = "jdbc:mysql://localhost:3306/test_db"
jdbc_properties = {
    "user": "root",
    "password": "Rinthya@1102",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from the MySQL table using JDBC
df = spark.read.jdbc(url=jdbc_url, table="test_table", properties=jdbc_properties)

# Show the DataFrame data
df.show()

# Stop the Spark session when done
spark.stop()
