import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[2] --jars spark-streaming-kafka-assembly_2.10-1.6.1.jar pyspark-shell'
import findspark
findspark.init()
os.getcwd()

import pyspark
sc = pyspark.SparkContext()
from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 1)
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql.types import *
schema = StructType([StructField("state", StringType(), True), StructField("account_length", DoubleType(), True), StructField("area_code", StringType(), True), StructField("phone_number", StringType(), True),     StructField("intl_plan", StringType(), True),     StructField("voice_mail_plan", StringType(), True),     StructField("number_vmail_messages", DoubleType(), True),     StructField("total_day_minutes", DoubleType(), True),     StructField("total_day_calls", DoubleType(), True),     StructField("total_day_charge", DoubleType(), True),     StructField("total_eve_minutes", DoubleType(), True),     StructField("total_eve_calls", DoubleType(), True),     StructField("total_eve_charge", DoubleType(), True),     StructField("total_night_minutes", DoubleType(), True),     StructField("total_night_calls", DoubleType(), True),     StructField("total_night_charge", DoubleType(), True),     StructField("total_intl_minutes", DoubleType(), True),     StructField("total_intl_calls", DoubleType(), True),     StructField("total_intl_charge", DoubleType(), True),     StructField("number_customer_service_calls", DoubleType(), True),     StructField("churned", StringType(), True)])

# import mllib's and graph/plot libs
import dateutil
import matplotlib.pyplot as plt
import seaborn as sb

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier

# In[5]:

import numpy as np
def takeAndPrint(time, rdd, num=1000):
	sqlContext = SQLContext(sc)
	result = []
	taken = rdd.take(num + 1)
	print("-------------------------------------------")
	print("Time: %s" % time)
	print("-------------------------------------------")
	for record in taken[:num]:
	    vals= tuple(record.split(",")) #[tuple(['Alice', '1'])]
	    result.append(vals) 
	    
	print(type(result))
	print(result)
	df = sqlContext.createDataFrame(result).collect()
	df.show()
	# Dataframe for MLLIB's
	# panda dataframe
	sample_data = df.sample(False, 0.5, 83).toPandas()
	sample_data.head()
	# find category and numerical variables
	numeric_cols = ["account_length", "number_vmail_messages", "total_day_minutes",
	            "total_day_calls", "total_day_charge", "total_eve_minutes",
	            "total_eve_calls", "total_eve_charge", "total_night_minutes",
	            "total_night_calls", "total_intl_minutes", "total_intl_calls",
	            "total_intl_charge"]

	categorical_cols = ["state", "international_plan", "voice_mail_plan", "area_code"]

	#some plots

	ax = sb.boxplot(x="churned", y="number_customer_service_calls", data=sample_data, palette="Set3")

	ax.set(xlabel="Churned", ylabel="Number of calls made to the customer service")
	plt.show()
	example_numeric_data = sample_data[["total_day_minutes", "total_day_calls",
	                                   "total_day_charge", "churned"]]
	sb.pairplot(example_numeric_data, hue="churned", palette="husl")
	plt.show()
	# correlation and heatmap
	corr = sample_data[["account_length", "number_vmail_messages", "total_day_minutes",
	                "total_day_calls", "total_day_charge", "total_eve_minutes",
	                "total_eve_calls", "total_eve_charge", "total_night_minutes",
	                "total_night_calls", "total_intl_minutes", "total_intl_calls", "total_intl_charge"]].corr()

	sb.heatmap(corr)
	reduced_numeric_cols = ["account_length", "number_vmail_messages", "total_day_calls",
	                    "total_day_charge", "total_eve_calls", "total_eve_charge",
	                    "total_night_calls", "total_intl_calls", "total_intl_charge"]

	label_indexer = StringIndexer(inputCol = 'churned', outputCol = 'label')
	plan_indexer = StringIndexer(inputCol = 'intl_plan', outputCol = 'intl_plan_indexed')

	assembler = VectorAssembler(
	inputCols = ['intl_plan_indexed'] + reduced_numeric_cols,
	outputCol = 'features')


	classifier = DecisionTreeClassifier(labelCol = 'label', featuresCol = 'features')

	pipeline = Pipeline(stages=[plan_indexer, label_indexer, assembler, classifier])

	(train, test) = df.randomSplit([0.7, 0.3])
	model = pipeline.fit(train)

	# Random forest
	from pyspark.mllib.tree import RandomForest
	model2 = RandomForest.trainClassifier(train, numClasses=2, 
	                                 numTrees=3, featureSubsetStrategy="auto",
	                                 impurity='gini', maxDepth=4, maxBins=32)
									 
									 
	# SVM needs some tweaking- not working as expected

	# ROC chart
	from pyspark.ml.evaluation import BinaryClassificationEvaluator

	predictions = model.transform(test)
	evaluator = BinaryClassificationEvaluator()
	auroc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})
    

kvs = KafkaUtils.createDirectStream(ssc, ["test2"], {"metadata.broker.list": "127.0.0.1:9092"})

lines = kvs.map(lambda x: x[1])
lines.foreachRDD(takeAndPrint)
ssc.start()
ssc.awaitTermination()



