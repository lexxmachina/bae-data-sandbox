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

checkpointDirectory = "./checkpoint/spark-streaming-consumer"

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("\t")
    return int(fields[0]), fields[1]

def modelPredict(rdd):
    """
    Return model predctions
    """ 
    predictions = model.predictAll(rdd.map(lambda x: (int(x[1]), int(x[2])))).map(lambda r: ((r[0], r[1]), round(r[2],1)))
    return predictions.join(rdd.map(lambda x: ((int(x[1]), int(x[2])), (float(x[3]), x[0]))))  
    
def updateRmse(newValue, runningRmse):  
    if runningRmse is None:
        runningRmse = 0
    return sqrt(runningRmse**2 + newValue**2)

def movieRec(rdd):
    users = rdd.map(lambda r: int(r[1])).collect()
    for user in users:
        recommendations =  model.recommendProducts(user,5)  
    return sc.parallelize(recommendations)
    # return recommendations
    # allMovies = movies.map(lambda x: x[0]).collect()
    # return rdd.map(lambda r: r[0], [m for m in allMovies])
    # predictions = model.predictAll(allMovies.map(lambda x: (userID, x))).collect()
    # predictions.map(lambda x: (x[0], x[1]), x[3]).map(lambda r: r.sorted)
    # return sorted(predictions, key=lambda x: x[2], reverse=True)[:50]
    
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

        global model
        model = MatrixFactorizationModel.load(sc, "./target/tmp/movieLensRecom")

        movies = sc.textFile("C:/Users/AJA35/Documents/Data Sandbox/real-time embedded analytics/data/ml-latest-small/movies.txt").map(parseMovie)
      	
        ratings = lines.map(lambda line: line.split(","))
        ratings.cache()
        ratings.pprint()

        predsAndRates = ratings.transform(lambda rdd: modelPredict(rdd))
        predsAndRates.pprint()

        # Root mean sqaured error
       
        # rmse = predsAndRates.transform(lambda rdd: computeRmse(rdd)) #.map(lambda r : {'RMSE': r})
        # rmse = predsAndRates.map(lambda x: (x[1][0] - x[1][1][0]) ** 2) \
        #     .reduce(add) \
        #     .map(lambda y : ('mse', y)) \
        #     .join(predsAndRates.count().map(lambda c : ('mse', c))) \
        #     .map(lambda x : sqrt(x[1][0]/x[1][1])) \
        #     .map(lambda r : {'tstamp': r[1], 'RMSE': r[0]})
        # rmse.pprint()

        # # rmseBase = predsAndRates.transform(lambda rdd: baselineRmse(rdd)).map(lambda r : {'RMSE_CLIM': r})
        # meanRating = predsAndRates.map(lambda r: sum(x[1][1][0])/length(x[1][1][0]))

        # rmseBase = predsAndRates.map(lambda x: (meanRating- x[1][1][0]) ** 2) \
        #     .reduce(add) \
        #     .map(lambda y : ('mse', y)) \
        #     .join(predsAndRates.count().map(lambda c : ('mse', c))) \
        #     .map(lambda x : sqrt(x[1][0]/x[1][1])) \
        #     .map(lambda r : {'tstamp': r[1], 'RMSE_base': r[0]})
        # rmseBase.pprint()

        # runningRmse = rmse.updateStateByKey(updateRmse)
        # runningRmse.pprint()  

        # Writing to ElasticSearch

        # predsAndRates_dict = predsAndRates.map(lambda r: (r[0][1], (r[0][0], r[1][0], r[1][1][0], abs(r[1][0] - r[1][1][0]),  r[1][1][1]))) \
        #     .transform(lambda rdd: rdd.join(movies.map(lambda r: (r[0], r[1])))) \
        #     .map(lambda r: {'tstamp': r[1][0][4], 'userID': r[1][0][0], 'movieID': r[0], 'movieTitle': r[1][1], 'predictedRating' : r[1][0][1], 'actualRating': r[1][0][2], 'absError': r[1][0][3]})
        # es_predsAndRates = predsAndRates_dict.map(lambda r : ('key', r))
        # es_predsAndRates.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
        #       outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
        #       keyClass="org.apache.hadoop.io.NullWritable", 
        #       valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",   
        #       conf={"es.resource": "movies/ratingPrediction"}))

        # es_rmse = rmse.map(lambda r: ('key', r))
        # es_rmse.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
        #       outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
        #       keyClass="org.apache.hadoop.io.NullWritable", 
        #       valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        #       conf={"es.resource": "movies/rmse"}))

        # es_rmseBase = rmseBase.map(lambda r: ('key', r))
        # es_rmseBase.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
        #       outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
        #       keyClass="org.apache.hadoop.io.NullWritable", 
        #       valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        #       conf={"es.resource": "movies/rmseBase"}))

        # Recommendations - create list of movies sorted by predicted ratings 

        # update model with each new batch
        # trainingData = ratings.map(lambda r: (r[1], r[2], r[3])).cache()
        # model = ALS.train(trainingData)

        # print(model.recommendProductsForUsers(5))
        recommendations = ratings.transform(lambda rdd: movieRec(rdd)) 
        recommendations.pprint()

ssc.start()
ssc.awaitTermination()