from pyspark.ml.feature import StringIndexer
from config.spark_config import create_spark_session
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier,GBTClassifier,RandomForestClassifier

from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator

def load_data(filepath):
    spark = create_spark_session()
    spark.catalog.clearCache()
    data = spark.read.csv(filepath,inferSchema=True,header=True)
    data.printSchema()
    data.describe().show()
    data.show()

    return data

def Assemble(data):

    assembler = VectorAssembler(inputCols=['Apps', 'Accept', 'Enroll', 'Top10perc', 'Top25perc', 'F_Undergrad', 'P_Undergrad', 'Outstate', 'Room_Board', 'Books', 'Personal', 'PhD', 'Terminal', 'S_F_Ratio', 'perc_alumni', 'Expend', 'Grad_Rate'], outputCol='features')
    output = assembler.transform(data)
    indexer = StringIndexer(inputCol='Private',outputCol='PrivateIndex')
    output_fixed = indexer.fit(output).transform(output)
    return output_fixed

def split_Assembled_Data(output):

    final_data = output.select('features', 'PrivateIndex')
    return final_data,final_data.randomSplit([0.7,0.3])

def Modals():
    filepath = 'Data/College.csv'
    data = load_data(filepath)
    # print(data.columns)
    output = Assemble(data)
    full_data,(train_data,test_data) = split_Assembled_Data(output)

    dtc = DecisionTreeClassifier(labelCol='PrivateIndex', featuresCol='features')
    rfc = RandomForestClassifier(labelCol='PrivateIndex', featuresCol='features')
    gbt = GBTClassifier(labelCol='PrivateIndex', featuresCol='features')

    dtc_model = dtc.fit(train_data)
    rfc_model = rfc.fit(train_data)
    gbt_model = gbt.fit(train_data)

    dtc_preds = dtc_model.transform(test_data)
    rfc_preds = rfc_model.transform(test_data)
    gbt_preds = gbt_model.transform(test_data)

    bin_eval = BinaryClassificationEvaluator(labelCol='PrivateIndex')
    print('DTC')
    print(bin_eval.evaluate(dtc_preds))

    print('RFC')
    print(bin_eval.evaluate(rfc_preds))

    gbt_eval = BinaryClassificationEvaluator(labelCol='PrivateIndex',rawPredictionCol='prediction')
    print('GBT')
    print(gbt_eval.evaluate(gbt_preds))

    acc_eval = MulticlassClassificationEvaluator(labelCol='PrivateIndex',metricName='accuracy')

    print('RFC')
    print(acc_eval.evaluate(rfc_preds))
    print('DTC')
    print(acc_eval.evaluate(dtc_preds))
    print('GBT')
    print(acc_eval.evaluate(gbt_preds))


Modals()