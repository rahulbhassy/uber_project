# config/spark_config.py
from pyspark.sql import SparkSession
import configparser


def create_spark_session():
    # Read the configuration file for any Spark settings
    config = configparser.ConfigParser()
    config.read('Shared/config.ini')

    # Initialize the Spark session
    spark = SparkSession.builder \
        .appName("ML") \
        .config("spark.jars",
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/delta-core_2.12-2.4.0.jar,"
                "file:///C:/Users/HP/uber_project/config/mysql-connector-j-8.0.33.jar"
                ) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:///C:/Users/HP/uber_project/Shared/log4j.properties") \
        .getOrCreate()

    return spark

def create_spark_session_jdbc():
    # Read the configuration file for any Spark settings
    config = configparser.ConfigParser()
    config.read('Shared/config.ini')

    # Initialize the Spark session
    spark = SparkSession.builder \
        .appName("UberETL") \
        .config("spark.jars",
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/delta-core_2.12-2.4.0.jar,"
                "file:///C:/Users/HP/uber_project/Data/mssql-jdbc-12.10.1.jre11.jar"
                ) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:///C:/Users/HP/uber_project/Shared/log4j.properties") \
        .getOrCreate()

    return spark




def create_spark_session_sedona():
    # Read the configuration file for any Spark settings
    config = configparser.ConfigParser()
    config.read('Shared/config.ini')

    # Initialize the Spark session
    spark = SparkSession.builder \
        .appName("UberETL") \
        .master("local[4]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.fraction", "0.7") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.jars",
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/delta-core_2.12-2.4.0.jar,"
                "file:///C:/Users/HP/uber_project/config/mysql-connector-j-8.0.33.jar,"
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/sedona-core-3.0_2.12-1.4.1.jar,"
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/sedona-sql-3.0_2.12-1.4.1.jar,"
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/jts-core-1.18.2.jar,"
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/geojson-jackson-1.2.1.jar,"
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/jts2geojson-0.16.1.jar,"
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/sedona-spark-shaded-3.0_2.12-1.6.1.jar,"
                "file:///C:/Spark/spark-3.4.4-bin-hadoop3/jars/geotools-wrapper-geotools-24.0.jar"

                ) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:///C:/Users/HP/uber_project/Shared/log4j.properties") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()

    return spark