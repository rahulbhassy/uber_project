
from config.spark_config import create_spark_session
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler,StringIndexer
from pyspark.ml.linalg import Vectors

def load_data():
    spark = create_spark_session()
    data = spark.read.csv("Data/cruise_ship_info.csv",inferSchema=True,header=True)

    indexer = StringIndexer(inputCol="Cruise_line",outputCol="Cruise_line_index")
    data = indexer.fit(data).transform(data)

    data.printSchema()

    assembler = VectorAssembler(inputCols=['Cruise_line_index','Age','Tonnage','passengers','length','cabins','passenger_density'],outputCol='feature')
    data = assembler.transform(data)

    final_data = data.select("feature","crew")
    final_data.show()
    return final_data.randomSplit([0.7,0.3])

def trainModel():
    train_data,test_data = load_data()
    train_data.describe().show()
    test_data.describe().show()

    lr  = LinearRegression(featuresCol="feature",labelCol="crew",predictionCol="predicted_crew")
    lrModel = lr.fit(train_data)

    # Print model coefficients and intercept
    print("Coefficients:", lrModel.coefficients)
    print("Intercept:", lrModel.intercept)

    # Model summary
    training_summary = lrModel.summary
    print("Root Mean Squared Error on training data:", training_summary.rootMeanSquaredError)

    ship_results = lrModel.evaluate(test_data)
    print(ship_results.rootMeanSquaredError)

    unlabeled_data = test_data.select("feature")
    predictions = lrModel.transform(unlabeled_data)
    predictions.show()
    compare_df = test_data.join(predictions,on="feature",how="inner")
    compare_df.show()


trainModel()
