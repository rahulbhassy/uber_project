# config/spark_config.py
from pyspark.sql import SparkSession
import configparser


def create_spark_session():
    # Read the configuration file for any Spark settings
    config = configparser.ConfigParser()
    config.read('config/config.ini')

    # Initialize the Spark session
    spark = SparkSession.builder \
        .appName("UberETL") \
        .config("spark.jars", r"C:\Spark\spark-3.4.4-bin-hadoop3\jars\delta-core_2.12-2.4.0.jar") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:C:/Users/HP/uber_project/config/log4j.properties") \
        .getOrCreate()

    return spark
