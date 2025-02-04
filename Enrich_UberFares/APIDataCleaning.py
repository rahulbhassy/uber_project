from config.spark_config import create_spark_session

def EnrichDataCleaning():
    spark = create_spark_session()
    filepath = "C:/Users/HP/uber_project/Data/Enriched_Distance_uberData/uberData.csv"
    uberData = spark.read.format("delta").load(filepath)
    filterData = uberData.filter(uberData.avg_temp.isNotNull() & uberData.precipitation.isNotNull())

    filterData.write \
        .format("delta") \
        .mode("overwrite") \
        .save("C:/Users/HP/uber_project/Data/Enriched/UberFares.csv")

    return filterData
EnrichDataCleaning()