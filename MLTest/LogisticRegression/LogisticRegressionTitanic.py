from config.spark_config import create_spark_session
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler,VectorIndexer,OneHotEncoder,StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
spark = create_spark_session()
filepath = 'C:/Users/HP/uber_project/MLTest/Data/titanic.csv'
df = spark.read.csv(filepath,inferSchema=True,header=True)


final_df = df.select(['Survived', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Fare', 'Embarked'])

final_df = final_df.na.drop()

gender_indexer = StringIndexer(inputCol='Sex',outputCol='SexIndex')
gender_encoder = OneHotEncoder(inputCol='SexIndex',outputCol='SexVec')

embark_indexer = StringIndexer(inputCol='Embarked',outputCol='EmbarkIndex')
embark_encoder = OneHotEncoder(inputCol='EmbarkIndex',outputCol='EmbarkVec')

assembler = VectorAssembler(inputCols=['Pclass','SexVec','EmbarkVec','Age','SibSp','Parch', 'Fare'],outputCol='feature')

lr_titanic = LogisticRegression(featuresCol='feature',labelCol='Survived')

pipeline = Pipeline(stages=[gender_indexer,embark_indexer,gender_encoder,embark_encoder,assembler,lr_titanic])

train_data,test_data = final_df.randomSplit([0.7,0.3])
fit_model = pipeline.fit(train_data)

results = fit_model.transform(test_data)

eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='Survived')

results.select('Survived','prediction').show()

auc = eval.evaluate(results)
print(auc)
