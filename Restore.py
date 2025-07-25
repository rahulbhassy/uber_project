from Shared.sparkconfig import create_spark_session

path = 'C:/Users/HP/uber_project/Data/Raw/uberfares/current/uberfares.delta'
spark = create_spark_session()

spark.sql(f"""
    RESTORE delta.`{path}`
    TO VERSION AS OF 2;
"""
)