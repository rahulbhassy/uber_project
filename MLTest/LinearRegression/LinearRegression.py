from config.spark_config import create_spark_session
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import DoubleType

# Create the Spark session
spark = create_spark_session()

# Load the training data
training = spark.read.csv("Data/training_data.csv", header=True, inferSchema=True)

# Cast columns to Double for safety (if they are not already numeric)
training = training.withColumn("Salary", training["Salary"].cast(DoubleType()))
training = training.withColumn("YearsExperience", training["YearsExperience"].cast(DoubleType()))

# Use VectorAssembler to create a vector column from the Salary column
assembler = VectorAssembler(inputCols=["Salary"], outputCol="features")
training = assembler.transform(training)

# Check the data
training.show(5)

# Set up the Linear Regression model
# Here we are predicting YearsExperience based on Salary
lr = LinearRegression(featuresCol="features", labelCol="YearsExperience", predictionCol="prediction")

# Fit the model
lrModel = lr.fit(training)

# Print model coefficients and intercept
print("Coefficients:", lrModel.coefficients)
print("Intercept:", lrModel.intercept)

# Model summary
training_summary = lrModel.summary
print("Root Mean Squared Error on training data:", training_summary.rootMeanSquaredError)

# Process test data in a similar fashion
test_data = spark.read.csv("Data/test_data.csv", header=True, inferSchema=True)
test_data = test_data.withColumn("Salary", test_data["Salary"].cast(DoubleType()))
test_data = test_data.withColumn("YearsExperience", test_data["YearsExperience"].cast(DoubleType()))
test_data = assembler.transform(test_data)

# Evaluate the model on test data
test_results = lrModel.evaluate(test_data)
print("Root Mean Squared Error on test data:", test_results.rootMeanSquaredError)

# For unlabeled data, if you want to predict YearsExperience given Salary:
# Assume the unlabeled data only contains Salary.
unlabeled_data = test_data.select("Salary")  # This is still a numeric column.
# Transform the unlabeled data so that it has the "features" vector.
unlabeled_data = assembler.transform(unlabeled_data)
predictions = lrModel.transform(unlabeled_data)
predictions.show()

spark.stop()