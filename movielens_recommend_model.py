from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from operator import add
from os.path import join, isfile, dirname
import sys
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import itertools
from math import sqrt

conf = SparkConf().setAppName('MovieLensRecommend').setMaster('local[4]')
sc = SparkContext(conf=conf)
spark = SQLContext(sc)

def parseRating(line):
    """
    Parses a rating record in MovieLens format userId::movieId::rating::timestamp .
    """
    fields = line.strip().split("::")
    return int(fields[0]), int(fields[1]), float(fields[2])

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("::")
    return int(fields[0]), fields[1]

def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])).join(data.map(lambda x: ((x[0], x[1]), x[2]))).values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

# ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
ratings = sc.textFile("C:/Users/AJA35/Documents/Data Sandbox/real-time embedded analytics/data/ml-1m/ratings.dat").map(parseRating)

numRatings = ratings.count()
print (numRatings)
numUsers = ratings.map(lambda r: r[0]).distinct().count()
numMovies = ratings.map(lambda r: r[1]).distinct().count()

print("Got %d ratings from %d users on %d movies." % (numRatings, numUsers, numMovies))

ratingsDF = spark.createDataFrame(ratings)

(training, validation, test) = ratingsDF.randomSplit([0.8, 0.1, 0.1])
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

# compare the best model with a naive baseline that always returns the mean rating
meanRating = training.unionAll(validation).rdd.map(lambda x: x[2]).mean()
baselineRmse = sqrt(test.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numTest)
improvement = (baselineRmse - testRmse) / baselineRmse * 100
print ("The best model improves the baseline by %.2f" % (improvement) + "%.")

# make personalized recommendations

# myRatedMovieIds = set([x[1] for x in myRatings])
# candidates = sc.parallelize([m for m in movies if m not in myRatedMovieIds])
# predictions = bestModel.predictAll(candidates.map(lambda x: (0, x))).collect()
# recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]

# print ("Movies recommended for you:")
# for i in xrange(len(recommendations)):
#     print ("%2d: %s" % (i + 1, movies[recommendations[i][1]])).encode('ascii', 'ignore')

# clean up
sc.stop()
