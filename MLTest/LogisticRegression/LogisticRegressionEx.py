
from config.spark_config import create_spark_session
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator , MulticlassClassificationEvaluator

spark = create_spark_session()
data = spark.read.format('libsvm').load('C:/Users/HP/uber_project/MLTest/Data/sample_libsvm_data.txt')

data.show()

train_data,test_data = data.randomSplit([0.7,0.3])

lr_model = LogisticRegression()
fitted_lr_model = lr_model.fit(train_data)

summary = fitted_lr_model.summary
summary.predictions.printSchema()

predictions_labels = fitted_lr_model.evaluate(test_data)
predictions_labels.show()

eval = BinaryClassificationEvaluator()
roc = eval.evaluate(predictions_labels.predictions)
print(roc)