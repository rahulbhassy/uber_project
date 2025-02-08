from config.spark_config import create_spark_session
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier,GBTClassifier,DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
spark = create_spark_session()
data = spark.read.format('libsvm').load('Data/sample_libsvm_data.txt')

train_data,test_data = data.randomSplit([0.7,0.3])

dtc = DecisionTreeClassifier(labelCol='label',featuresCol='features')
rfc = RandomForestClassifier(labelCol='label',featuresCol='features',numTrees=100)
gbt = GBTClassifier(labelCol='label',featuresCol='features')

dtc_model = dtc.fit(train_data)
rfc_model = rfc.fit(train_data)
gbt_model = gbt.fit(train_data)

dtc_preds = dtc_model.transform(test_data)
rfc_preds = rfc_model.transform(test_data)
gbt_preds = gbt_model.transform(test_data)

acc_eval = MulticlassClassificationEvaluator(metricName='accuracy')
print('DTC Accuracy')
print(acc_eval.evaluate(dtc_preds))
print('RFC Accuracy')
print(acc_eval.evaluate(rfc_preds))
print('GBT Accuracy')
print(acc_eval.evaluate(gbt_preds))

