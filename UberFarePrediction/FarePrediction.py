from pyspark.ml.evaluation import RegressionEvaluator,BinaryClassificationEvaluator
from config.spark_config import create_spark_session_sedona
from sedona.spark import SedonaContext
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler,StringIndexer


def data_loader():
    spark = create_spark_session_sedona()
    SedonaContext.create(spark)
    filepath = 'C:/Users/HP/uber_project/Data/EnrichedGeoSpatial/spatial_analysis_delta'
    data = spark.read.format('delta').load(filepath)

    borough_indexer = StringIndexer(inputCol='borough',outputCol='boroughIndex')
    data = borough_indexer.fit(data).transform(data)

    assembler = VectorAssembler(inputCols=['passenger_count','pickup_hour','pickup_day','avg_temp','precipitation','distance_km','boroughIndex'],outputCol='features')
    data = assembler.transform(data)

    final_data = data.select('features','fare_amount')
    return spark,final_data.randomSplit([0.9,0.1])


def EvaluateModel(predictions):
    # Show a summary of the relevant columns
    predictions.select("fare_amount", "predicted_fare_amount").describe().show()

    # Compute RMSE
    evaluator_rmse = RegressionEvaluator(
        labelCol="fare_amount",
        predictionCol="predicted_fare_amount",
        metricName="rmse"
    )
    rmse = evaluator_rmse.evaluate(predictions)

    # Compute R-squared (as a measure of accuracy for regression)
    evaluator_r2 = RegressionEvaluator(
        labelCol="fare_amount",
        predictionCol="predicted_fare_amount",
        metricName="r2"
    )
    r2 = evaluator_r2.evaluate(predictions)

    print(f"Root Mean Squared Error (RMSE): {rmse}")
    print(f"R-squared (R2): {r2}")
    return rmse, r2


def modal(train_data,test_data):


    # Define the RandomForestRegressor model
    rfr = RandomForestRegressor(
        labelCol='fare_amount',
        featuresCol='features',
        numTrees=100,
        maxDepth=5,
        predictionCol='predicted_fare_amount'
    )

    # Train the model
    fitted_model = rfr.fit(train_data)

    # Generate predictions on the test set
    predictions = fitted_model.transform(test_data)

    # Evaluate using RMSE (direct evaluation)
    evaluator = RegressionEvaluator(
        labelCol="fare_amount",
        predictionCol="predicted_fare_amount",
        metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"Direct evaluation - Root Mean Squared Error (RMSE): {rmse}")

    # Evaluate the model further (including R² as "accuracy")
    EvaluateModel(predictions)

    # Show a comparison DataFrame
    compare_df = predictions.select("features", "fare_amount", "predicted_fare_amount")
    compare_df.show()

    return fitted_model

def main():
    spark,(train_data, test_data) = data_loader()
    model = modal(train_data,test_data)
    model.write().overwrite().save("models/uber_fare_predictor")
    spark.stop()

if __name__ == "__main__":
    main()
