from config.spark_config import create_spark_session_sedona
from pyspark.ml.feature import StringIndexer, VectorAssembler, VectorIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from sedona.spark import SedonaContext
from pyspark.sql.functions import col, round

def load_and_preprocess_data():
    # Create Spark session
    spark = create_spark_session_sedona()
    SedonaContext.create(spark)

    # Load data (assumes Delta format as before)
    filepath = 'C:/Users/HP/uber_project/Data/EnrichedGeoSpatial/spatial_analysis_delta'
    data = spark.read.format('delta').load(filepath)

    # Index the categorical "borough" column
    boroughIndexer = StringIndexer(inputCol='borough', outputCol='boroughIndex', handleInvalid="keep")

    # Assemble all features into one vector
    feature_cols = ['passenger_count', 'pickup_hour', 'pickup_day', 'avg_temp', 'precipitation', 'distance_km',
                    'boroughIndex']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')

    # Automatically index features that have few distinct values
    vectorIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4)

    # Build the preprocessing pipeline
    pipeline = Pipeline(stages=[boroughIndexer, assembler, vectorIndexer])
    processed_data = pipeline.fit(data).transform(data)

    # Select final columns (rename for clarity)
    final_data = processed_data.select('indexedFeatures', 'fare_amount') \
        .withColumnRenamed('indexedFeatures', 'features')
    return final_data, spark


def train_and_evaluate(data):
    # Split data into training and testing sets with a fixed seed
    train_data, test_data = data.randomSplit([0.9, 0.1], seed=42)

    # Define the RandomForestRegressor
    rf = RandomForestRegressor(
        labelCol='fare_amount',
        featuresCol='features',
        predictionCol='predicted_fare_amount'
    )

    # Create a Pipeline with the RF model (preprocessing is already done)
    pipeline = Pipeline(stages=[rf])

    # Define a hyperparameter grid to tune numTrees and maxDepth
    paramGrid = (ParamGridBuilder()
                 .addGrid(rf.numTrees, [100, 150, 200])
                 .addGrid(rf.maxDepth, [10, 15, 20])
                 .build())

    # Use RegressionEvaluator for RMSE
    evaluator = RegressionEvaluator(
        labelCol="fare_amount",
        predictionCol="predicted_fare_amount",
        metricName="rmse"
    )

    # Set up cross-validation with 3 folds for hyperparameter tuning
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3,
        seed=42
    )

    # Fit the model on the training data
    cvModel = cv.fit(train_data)

    # Make predictions on the test data
    predictions = cvModel.transform(test_data)

    # Evaluate using RMSE and R2
    rmse = evaluator.evaluate(predictions)
    r2 = RegressionEvaluator(
        labelCol="fare_amount",
        predictionCol="predicted_fare_amount",
        metricName="r2"
    ).evaluate(predictions)

    print(f"Optimized Model - RMSE: {rmse}, R2: {r2}")

    # Show some predictions for inspection
    predictions.select("fare_amount", "predicted_fare_amount", "features").show(10, truncate=False)

    # Display feature importances from the best RF model
    best_rf_model = cvModel.bestModel.stages[-1]
    print("Feature Importances:")
    print(best_rf_model.featureImportances)

    # Create a comparison DataFrame: actual fare, predicted fare, and the error
    comparison = predictions.select(
        col("fare_amount").alias("Actual_Fare"),
        round(col("predicted_fare_amount"), 2).alias("Predicted_Fare"),
        round(col("fare_amount") - col("predicted_fare_amount"), 2).alias("Error")
    )
    print("Comparison of Actual vs. Predicted Fare Amount:")
    comparison.show(10, truncate=False)


def main():
    data, spark = load_and_preprocess_data()
    train_and_evaluate(data)
    spark.stop()


if __name__ == "__main__":
    main()

