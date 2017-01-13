from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.listener import StreamingListener

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import itertools
from operator import add
from math import sqrt
import numpy as np

checkpointDirectory = "./checkpoint/spark-streaming-consumer"

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("::")
    return int(fields[0]), fields[1]

def modelPredict(rdd):
    """
    Return model predctions
    """ 
    predictions = model.predictAll(rdd.map(lambda x: (int(x[1]), int(x[2])))).map(lambda r: ((r[0], r[1]), round(r[2],1)))
    return predictions.join(rdd.map(lambda x: ((int(x[1]), int(x[2])), (float(x[3]), x[0]))))  
    
def computeRmse(rdd) :
    arr = rdd.map(lambda x: (x[1][0], x[1][1][0])).collect()
    se = [(x[0] - x[1])**2 for x in arr]
    return sc.parallelize([sum(se)/float(len(se))])
    
def updateRmse(newValue, runningRmse):  
    if runningRmse is None:
        runningRmse = 0
    return sqrt(runningRmse**2 + newValue**2)

def movieRec(rdd):
    users = rdd.map(lambda r: int(r[1])).collect()
    moviesList = movies.collect()
    recommendations = []
    for user in users:
        recs = model.recommendProducts(user,5)
        recTitle = []
        for rec in recs:
            ind = [y[0] for y in moviesList].index(rec.product)
            recTitle.append(rec + (moviesList[ind][1],))
        recommendations.append(recTitle)
    return sc.parallelize([item for item in recommendations])
    
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sys.argv[0] <zk> <topic>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="StreamingKafkaConsumerSaveToElastic")
    ssc = StreamingContext(sc, 10)
    # ssc.checkpoint(checkpointDirectory)  # set checkpoint directory

    zkQuorum, topic = sys.argv[1:]

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})    
    lines = kvs.map(lambda x: x[1])

    # lines.pprint()
    lines.count().pprint() 

    if lines.transform(lambda rdd: rdd.first()) :

        model = MatrixFactorizationModel.load(sc, "./target/tmp/movieLensRecom")

        movies = sc.textFile("C:/Users/AJA35/Documents/Data Sandbox/real-time embedded analytics/data/ml-1m/movies.dat").map(parseMovie)
      	
        ratings = lines.map(lambda line: line.split(","))
        ratings.cache()
        ratings.pprint()

        predsAndRates = ratings.transform(lambda rdd: modelPredict(rdd))
        predsAndRates.pprint()

        predsAndRates_dict = predsAndRates.map(lambda r: (r[0][1], (r[0][0], r[1][0], r[1][1][0], abs(r[1][0] - r[1][1][0]),  r[1][1][1]))) \
            .transform(lambda rdd: rdd.join(movies.map(lambda r: (r[0], r[1])))) \
            .map(lambda r: {'tstamp': r[1][0][4], 'userID': r[1][0][0], 'movieID': r[0], 'movieTitle': r[1][1], 'predictedRating' : r[1][0][1], 'actualRating': r[1][0][2], 'absError': r[1][0][3]})
        predsAndRates_dict.pprint()

    # Root mean sqaured error
        rmse = predsAndRates.transform(lambda rdd: computeRmse(rdd)).map(lambda r : {'RMSE': r})
        # rmse = predsAndRates.map(lambda x: (x[1][0] - x[1][1][0]) ** 2) \
        #     .reduce(add) \
        #     .map(lambda y : ('mse', y)) \
        #     .join(predsAndRates.count().map(lambda c : ('mse', c))) \
        #     .map(lambda x : sqrt(x[1][0]/x[1][1]))  \
        #     .map(lambda r : {'RMSE': r})
        rmse.pprint()

        # BASELINE RMSE could be broadcast variable (i.e. persisted on nodes)
        rmseBase = predsAndRates.transform(lambda rdd: baselineRmse(rdd)).map(lambda r : {'RMSE_Base': r})
        # meanRating = predsAndRates.map(lambda r: sum(x[1][1][0])/length(x[1][1][0]))

        # rmseBase = predsAndRates.map(lambda x: (meanRating- x[1][1][0]) ** 2) \
        #     .reduce(add) \
        #     .map(lambda y : ('mse', y)) \
        #     .join(predsAndRates.count().map(lambda c : ('mse', c))) \
        #     .map(lambda x : sqrt(x[1][0]/x[1][1])) \
        #     .map(lambda r : {'RMSE_base': r})
        # rmseBase.pprint()

        # runningRmse = rmse.updateStateByKey(updateRmse)
        # runningRmse.pprint()  

    # Recommendations - create list of movies sorted by predicted ratings 

        recommendations = ratings.transform(lambda rdd: movieRec(rdd)) \
            .map(lambda r: (r[0][0], str([(r[0][1], r[0][3], r[0][2]), (r[1][1], r[1][3], r[1][2]), (r[2][1], r[2][3], r[2][2]), (r[3][1], r[3][3], r[3][2]), (r[4][1], r[4][3], r[4][2])]))) \
            .join(ratings.map(lambda r: (int(r[1]), r[0]))) \
            .map(lambda r: {'tstamp': r[1][1], 'user': r[0], 'top5recommendations': r[1][0]})
        recommendations.pprint()

        # update model with each new batch

        # trainingData = ratings.map(lambda r: (r[1], r[2], r[3])).cache()
        # model = ALS.train(trainingData)

    # Writing to ElasticSearch

        es_predsAndRates = predsAndRates_dict.map(lambda r : ('key', r))
        es_predsAndRates.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
              outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
              keyClass="org.apache.hadoop.io.NullWritable", 
              valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",   
              conf={"es.resource": "movies/ratingPrediction"}))

        es_rmse = rmse.map(lambda r: ('key', r))
        es_rmse.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
              outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
              keyClass="org.apache.hadoop.io.NullWritable", 
              valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
              conf={"es.resource": "movies/rmse"}))

        # es_rmseBase = rmseBase.map(lambda r: ('key', r))
        # es_rmseBase.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
        #       outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
        #       keyClass="org.apache.hadoop.io.NullWritable", 
        #       valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        #       conf={"es.resource": "movies/rmseBase"}))

        es_recommendations = recommendations.map(lambda r: ('key', r))
        es_recommendations.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",   
            conf={"es.resource": "movies/recommendations"}))      

ssc.start()
ssc.awaitTermination()