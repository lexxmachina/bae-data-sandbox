from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql.types import *
from operator import add
from os.path import join, isfile, dirname
import sys
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import itertools
from math import sqrt

conf = SparkConf().setAppName('MovieLensRecommend').setMaster('local[4]')
sc = SparkContext(conf=conf)
spark = SQLContext(sc)

def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

# Load and parse the data
movieData = sc.textFile("C:/Users/AJA35/Documents/Data Sandbox/real-time embedded analytics/data/ml-latest-small/movies.txt")
ratingsData =  sc.textFile("C:/Users/AJA35/Documents/Data Sandbox/real-time embedded analytics/data/ml-latest-small/ratings.txt")

movieStats = movieData.map(lambda x: int(x.split(' ')[0])).stats()
ratingsStats = ratingsData.map(lambda x: int(x.split(' ')[1])).stats()

header = movieData.first()
lines = movieData.filter(lambda row: row != header).map(lambda x: x.split('\t'))
movies = lines.map(lambda l: (int(l[0]), str(l[1]), str(l[2].split('|'))))
showMovieCount = movies.map(lambda p: p[1]).countByValue()

header = ratingsData.first()
lines = ratingsData.filter(lambda row: row != header).map(lambda x: x.split('\t'))
# ratingsRDD = lines.map(lambda l: Row(userID=int(l[0]), movieID=int(l[1]), rating=float(l[2])))
ratingsRDD = lines.map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
userCount= ratingsRDD.map(lambda p: p[0]).countByValue()
movieCount = ratingsRDD.map(lambda p: p[1]).countByValue()

ratings = spark.createDataFrame(ratingsRDD)

(training, validation, test) = ratings.randomSplit([0.8, 0.1, 0.1])
numTraining = training.count()
numValidation = validation.count() 
numTest = test.count()

print ("Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest))

# Build the recommendation model using Alternating Least Squares
ranks = [8, 12]
lambdas = [1.0, 10.0]
numIters = [10, 20]
bestModel = None
bestValidationRmse = float("inf")
bestRank = 0
bestLambda = -1.0
bestNumIter = -1

for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
    model = ALS.train(training, rank, numIter, lmbda)
    validationRmse = computeRmse(model, validation, numValidation)
    print("RMSE (validation) = %f for the model trained with " % validationRmse + "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter))
    if (validationRmse < bestValidationRmse):
        bestModel = model
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lmbda
        bestNumIter = numIter

testRmse = computeRmse(bestModel, test, numTest)

# evaluate the best model on the test set
print("The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse))

# Save and load model
bestModel.save(sc, "target/tmp/movieLensRecommend")
sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")

# compare the best model with a naive baseline that always returns the mean rating
meanRating = training.unionAll(validation).rdd.map(lambda x: x[2]).mean()
baselineRmse = sqrt(test.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numTest)
improvement = (baselineRmse - testRmse) / baselineRmse * 100
print ("The best model improves the baseline by %.2f" % (improvement) + "%.")

# make personalized recommendations

myRatedMovieIds = set([x[1] for x in myRatings])
candidates = sc.parallelize([m for m in movies if m not in myRatedMovieIds])
predictions = bestModel.predictAll(candidates.map(lambda x: (0, x))).collect()
recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]

print ("Movies recommended for you:")
for i in xrange(len(recommendations)):
    print ("%2d: %s" % (i + 1, movies[recommendations[i][1]])).encode('ascii', 'ignore')

# clean up
sc.stop()
