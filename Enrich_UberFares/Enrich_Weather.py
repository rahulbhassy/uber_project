from Enrich_UberFares.WeatherAPI import enrich_with_weather
from config.spark_config import create_spark_session
import os
# Set the PYSPARK_PYTHON environment variable
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

spark = create_spark_session()
filepath = "C:/Users/HP/uber_project/Data/Cleaned_UberFares/UberFares.csv"
uberData = spark.read.format("delta").load(filepath)
enriched_uberData = enrich_with_weather(uberData)

enriched_uberData.write \
    .format("delta") \
    .mode("overwrite") \
    .save("C:/Users/HP/uber_project/Data/Enriched_Weather_uberData/uberData.csv")
enriched_uberData.show()