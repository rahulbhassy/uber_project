from pyspark.sql.functions import round,count,avg


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
    fare_Data.show()
    fare_Data.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "uber_analysis") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .mode("overwrite") \
        .save()
