from config.spark_config import create_spark_session
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler


def load_data():
    spark = create_spark_session()
    data = spark.read.csv("Data/Ecommerce_Customers.csv",inferSchema=True,header=True)
    data.printSchema()
    assembler = VectorAssembler(inputCols=['Avg Session Length','Time on App','Time on Website','Length of Membership'],outputCol='features')
    data = assembler.transform(data)
    data.printSchema()

    final_data = data.select("features","Yearly Amount Spent")

    return final_data.randomSplit([0.7,0.3])

def LRModel():

    train_data,test_data = load_data()
    train_data.describe().show()
    test_data.describe().show()

    lr = LinearRegression(labelCol="Yearly Amount Spent")
    lrmodel = lr.fit(train_data)

    test_results = lrmodel.evaluate(test_data)
    test_results.residuals.show()
    print(test_results.rootMeanSquaredError)
    print(test_results.r2)

    unlabeled_data = test_data.select('features')
    predictions = lrmodel.transform(unlabeled_data)
    predictions.show()
LRModel()

