from pyspark.sql.functions import round,count,avg
from config.spark_config import create_spark_session

# Define JDBC connection properties
jdbc_url = "jdbc:mysql://localhost:3306/uber_analysis"
jdbc_properties = {
    "user": "root",
    "password": "Rinthya@1102",
    "driver": "com.mysql.cj.jdbc.Driver"
}

def fareAnalysis(uberData):

    fare_Data = uberData.groupBy("pickup_hour").agg(
        round(avg("fare_amount"), 2).alias("Average_Fare"),
        round(avg("distance_km"), 2).alias("Average_Distance"),
        count("trip_id").alias("Trip_Count")
    )
    fare_Data.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "uber_analysis") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .mode("overwrite") \
        .save()

def getFareTableData():

    # Define your custom query.
    # Note: Wrap your query in parentheses and alias it (e.g., as tmp)
    custom_query = "(SELECT * FROM uber_analysis) AS tmp"
    spark = create_spark_session()
    # Load data using the custom query.
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", custom_query) \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()

    # Show the DataFrame content.
    df.show(truncate=False)



getFareTableData()