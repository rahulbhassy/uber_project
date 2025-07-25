from Shared.sparkconfig import create_spark_session

spark = create_spark_session()
path = 'C:/Users/HP/uber_project/Data/Enrich/Enriched/uberfares/current/uberfares.delta'

df = spark.read.format("delta").load(path)
print(df.count())
spark.stop()