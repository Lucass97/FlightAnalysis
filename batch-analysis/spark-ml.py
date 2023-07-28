from pyspark.ml.classification import LinearSVC, RandomForestClassifier, RandomForestClassificationModel, LinearSVCModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, dayofweek, dayofmonth
from pyspark.ml.feature import StandardScaler, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.pipeline import Pipeline, PipelineModel, PipelineWriter, PipelineReader, PipelineModelReader, PipelineModelWriter


cluster_seeds = ['localhost:9042', 'localhost:9043']

spark = SparkSession \
    .builder \
    .appName("Flight Batch Analysis") \
    .config("spark.cassandra.connection.host", ','.join(cluster_seeds)) \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

df = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2017.csv")

"""
========================================================================================
Delay Prediction
========================================================================================
"""

df_delay = df.select(month("FL_DATE").alias("MONTH"),
          dayofmonth("FL_DATE").alias("DAYOFMONTH"),
          dayofweek("FL_DATE").alias("DAYOFWEEK"),
          "OP_CARRIER", "ORIGIN", "DEST",
          col("ARR_DELAY").cast("FLOAT"), col("DISTANCE").cast("float")) \
    .filter(col("ARR_DELAY").isNotNull()) \
    .filter(col("DISTANCE").isNotNull()) \
    .withColumn("IS_DELAY", (col("ARR_DELAY") > 0).cast("int")) \
    .sample(fraction=0.1)

# Preprocessing
indexer = StringIndexer(inputCols = ["OP_CARRIER", "ORIGIN", "DEST"], outputCols =["INDEX_CARRIER", "INDEX_ORIGIN", "INDEX_DEST"])
oneHotEncoder = OneHotEncoder(inputCols=["INDEX_CARRIER", "INDEX_ORIGIN", "INDEX_DEST"], outputCols=["ONEHOT_CARRIER", "ONEHOT_ORIGIN", "ONEHOT_DEST"])
assembler = VectorAssembler(inputCols=["MONTH", "DAYOFMONTH", "DAYOFWEEK", "ONEHOT_CARRIER", "ONEHOT_ORIGIN", "ONEHOT_DEST", "DISTANCE"], outputCol="FEATURES")
scaler = StandardScaler(inputCol="FEATURES", outputCol="SCALED_FEATURES", withStd=True, withMean=True)

train_data, test_data = df_delay.randomSplit([0.8, 0.2], seed=1234)
#lr = RandomForestClassifier(labelCol='IS_DELAY', featuresCol='SCALED_FEATURES')
lr = LinearSVC(maxIter=10, regParam=0.1, labelCol="IS_DELAY", featuresCol="SCALED_FEATURES")
my_stages = [indexer, oneHotEncoder, assembler, scaler, lr]
pipeline = Pipeline(stages=my_stages)

pipelineFit = pipeline.fit(train_data)

result = pipelineFit.transform(test_data)
predictionAndLabels = result.select("prediction", "IS_DELAY")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy").setLabelCol("IS_DELAY")

print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))

predictionAndLabels.show(100)

pipeline.write().overwrite().save("hdfs://localhost:9000/output/models/delay_pipeline")

"""
========================================================================================
Cancellation Prediction
========================================================================================
"""

df_cancellation = df.select(month("FL_DATE").alias("MONTH"),
                                         dayofmonth("FL_DATE").alias("DAYOFMONTH"),
                                         dayofweek("FL_DATE").alias("DAYOFWEEK"),
                                         "OP_CARRIER", "ORIGIN", "DEST",
                                         col("DISTANCE").cast("FLOAT"), col("CANCELLED").cast("int")) \
                                        .filter(col("CANCELLED").isNotNull()) \
                                        .filter(col("DISTANCE").isNotNull()) \
                                        .sample(fraction=0.1)


# Preprocessing
indexer = StringIndexer(inputCols = ["OP_CARRIER", "ORIGIN", "DEST"], outputCols =["INDEX_CARRIER", "INDEX_ORIGIN", "INDEX_DEST"])
oneHotEncoder = OneHotEncoder(inputCols=["INDEX_CARRIER", "INDEX_ORIGIN", "INDEX_DEST"], outputCols=["ONEHOT_CARRIER", "ONEHOT_ORIGIN", "ONEHOT_DEST"])
assembler = VectorAssembler(inputCols=["MONTH", "DAYOFMONTH", "DAYOFWEEK", "ONEHOT_CARRIER", "ONEHOT_ORIGIN", "ONEHOT_DEST", "DISTANCE"], outputCol="FEATURES")
scaler = StandardScaler(inputCol="FEATURES", outputCol="SCALED_FEATURES", withStd=True, withMean=True)

train_data, test_data = df_cancellation.randomSplit([0.8, 0.2], seed=1234)

#lr = RandomForestClassifier(labelCol='CANCELLED', featuresCol='SCALED_FEATURES')
lr = LinearSVC(maxIter=10, regParam=0.1, labelCol="CANCELLED", featuresCol="SCALED_FEATURES")
my_stages = [indexer, oneHotEncoder, assembler, scaler, lr]
pipeline = Pipeline(stages=my_stages)

pipelineFit = pipeline.fit(train_data)

result = pipelineFit.transform(test_data)
predictionAndLabels = result.select("prediction", "CANCELLED")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy").setLabelCol("CANCELLED")

print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))

predictionAndLabels.show(100)

pipeline.write().overwrite().save("hdfs://localhost:9000/output/models/cancellation_pipeline")

