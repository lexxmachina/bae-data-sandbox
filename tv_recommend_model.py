from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql.types import *
from operator import add
import sys
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

conf = SparkConf().setAppName('TVRecommend').setMaster('local[4]')
sc = SparkContext(conf=conf)
spark = SQLContext(sc)

# Load and parse the data

tvViewingData = sc.textFile("C:/Users/AJA35/Documents/Data Sandbox/real-time embedded analytics/data/fct_events.csv")

header = tvViewingData.first()
lines = tvViewingData.filter(lambda row: row != header).map(lambda x: x.split(','))
# showUser = lines.map(lambda p: (p[0], int(p[1]), int(p[2])))
showUserCount = showUser.map(lambda p: p[1]).countByValue()

showUserRDD = lines.map(lambda p: Row(show=int(p[1]), user=int(p[2])))
showCount= showUserRDD.map(lambda p: p[0]).countByValue()
userCount = showUserRDD.map(lambda p: p[1]).countByValue()

showUser = spark.createDataFrame(showUserRDD)
# df = spark.createDataFrame([(0, 0, 4.0), (0, 1, 2.0), (1, 1, 3.0), (1, 2, 4.0), (2, 1, 1.0), (2, 2, 5.0)],["user", "item", "rating"])

(training, test) = showUser.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# als = ALS(maxIter=5, regParam=0.01, implicitPrefs=True, userCol="user", itemCol="show", ratingCol="")
# model = als.fit(training)
rank = 10
numIterations = 10
model = ALS.trainImplicit(training, rank, numIterations, alpha=0.01)
predictions = sorted(model.transform(test).collect(), key=lambda r: r[0])

# Save and load model
model.save(sc, "target/tmp/myCollaborativeFilter")
sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
# $example off$
