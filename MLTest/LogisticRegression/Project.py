from openpyxl.styles.builtins import output

from config.spark_config import create_spark_session
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler


def load_data(filepath):
    spark = create_spark_session()
    spark.catalog.clearCache()
    ##filepath = 'C:/Users/HP/uber_project/MLTest/Data/customer_churn.csv'
    data = spark.read.csv(filepath,inferSchema=True,header=True)
    data.printSchema()
    data.describe().show()
    data.show()

    return data

def Assemble(data):

    assembler = VectorAssembler(inputCols=['Age', 'Total_Purchase', 'Years', 'Num_Sites'], outputCol='features')
    return assembler.transform(data)

def split_Assembled_Data(output):

    final_data = output.select('features', 'Churn')
    return final_data,final_data.randomSplit([0.7,0.3])


def EvaluateModel(model,test_data):
    training_sum = model.summary
    training_sum.predictions.describe().show()

    pred_labels = model.evaluate(test_data)
    pred_labels.predictions.show()

    eval = BinaryClassificationEvaluator(labelCol="Churn", rawPredictionCol='prediction')
    auc = eval.evaluate(pred_labels.predictions)
    print(auc)


def LRmodel():
    filepath = 'C:/Users/HP/uber_project/MLTest/Data/customer_churn.csv'
    data = load_data(filepath)
    output = Assemble(data)
    data,(train_data,test_data) = split_Assembled_Data(output)

    lr_model = LogisticRegression(labelCol='Churn',featuresCol='features')
    fit_model = lr_model.fit(train_data)
    EvaluateModel(fit_model,test_data)

    return lr_model.fit(data)

def predict():
    prediction_model = LRmodel()
    filepath = 'C:/Users/HP/uber_project/MLTest/Data/new_customers.csv'
    new_data = load_data(filepath)
    output = Assemble(new_data)
    output.printSchema()

    results = prediction_model.transform(output)
    results.show()

predict()