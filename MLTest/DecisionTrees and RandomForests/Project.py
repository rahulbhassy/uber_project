
from config.spark_config import create_spark_session
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier,GBTClassifier,RandomForestClassifier

def load_data(filepath):
    spark = create_spark_session()
    spark.catalog.clearCache()
    data = spark.read.csv(filepath,inferSchema=True,header=True)
    data.printSchema()
    data.describe().show()
    data.show()

    return data

def Assemble(data):
    assembler = VectorAssembler(inputCols=['A','B','C','D'],outputCol='features')
    output= assembler.transform(data)
    return output

def model():

    filepath = 'Data/dog_food.csv'
    data = load_data(filepath)
    output = Assemble(data)
    final_data = output.select('features','Spoiled')

    rfc = RandomForestClassifier(labelCol='Spoiled',featuresCol='features')

    rfc_model = rfc.fit(final_data)
    final_data.head()
    print(rfc_model.featureImportances)

model()