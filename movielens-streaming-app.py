from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.listener import StreamingListener

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import itertools
from operator import add
from math import sqrt

checkpointDirectory = "./checkpoint/spark-streaming-consumer"
# Function to create and setup a new StreamingContext

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("\t")
    return int(fields[0]), fields[1]
    
def updateRmse(newValue, runningRmse):  
    if runningRmse is None:
        runningRmse = 0
    return sqrt(runningRmse**2 + newValue**2)

# def movieRec(usersRDD):
#     users = usersRDD.map(lambda x : x[0]).collect()
#     for user in users:
#         preds = model.predictAll(movies.map(lambda x: (user, x))).map(lambda r: ('Recommenations', r[0], r[1], r[2])).sortByKey(lambda r: r[2])
#     return preds

    # users.map(lambda r: (r)).transform(lambda rdd: rdd.rightOuterJoin(movies.map(lambda r: (r[0], r[1]))))
    # predictions = bestModel.predictAll(candidates.map(lambda x: (0, x))).collect()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sys.argv[0] <zk> <topic>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="StreamingKafkaConsumerSaveToElastic")
    ssc = StreamingContext(sc, 5)
    # ssc.addStreamingListener(streamingListener)
    # ssc.checkpoint("./checkpoint/spark-streaming-consumer")   

    zkQuorum, topic = sys.argv[1:]

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})  
    lines = kvs.map(lambda x: x[1])
    lines.pprint()
    lines.count().pprint()
    
    model = MatrixFactorizationModel.load(sc, "./target/tmp/movieLensRecom")
    movies = sc.textFile("C:/Users/AJA35/Documents/Data Sandbox/real-time embedded analytics/data/ml-latest-small/movies.txt").map(parseMovie)
  	
    ratings = lines.map(lambda line: line.split(","))
    ratings.cache()
    # ratings.pprint()
    
    predictions = ratings.map(lambda r: (int(r[1]),int(r[2]))).transform(lambda rdd: model.predictAll(rdd)).map(lambda r: (r[0], r[1], round(r[2],1)))
    # predictions.pprint()

    predsAndRates = predictions.map(lambda x: (str(x[0]) + ',' + str(x[1]), x[2])).join(ratings.map(lambda r: (r[1] + ',' + r[2], (float(r[3]), r[0]))))
    predsAndRates.pprint()

    # Evaluate predictions

    rmse = predsAndRates.map(lambda x: (x[1][0] - x[1][1][0]) ** 2) \
        .reduce(add) \
        .map(lambda y : ('mse', y)) \
        .join(predsAndRates.count().map(lambda c : ('mse', c))) \
        .map(lambda x : sqrt(x[1][0]/x[1][1])) \
        .map(lambda r : {'RMSE': r})
    rmse.pprint()

    # runningRmse = rmse.updateStateByKey(updateRmse)
    # runningRmse.pprint()  

    # Writing to ElasticSearch

    predsAndRates_dict = predsAndRates.map(lambda r: (int(r[0].split(",")[1]), (r[0].split(",")[0], r[1][0], r[1][1][0], r[1][1][1]))) \
        .transform(lambda rdd: rdd.join(movies.map(lambda r: (int(r[0]), r[1])))) \
        .map(lambda r: {'tstamp': r[1][0][3], 'userID': r[1][0][0], 'movieID': r[0], 'movieTitle': r[1][1], 'predictedRating' : r[1][0][1], 'actualRating': r[1][0][2]})
    es_predsAndRates = predsAndRates_dict.map(lambda r : ('key', r))
    es_predsAndRates.pprint()

    es_predsAndRates.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
          outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
          keyClass="org.apache.hadoop.io.NullWritable", 
          valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
          conf={"es.resource": "movies/ratingPrediction"}))

    es_rmse = rmse.map(lambda r: ('key', r))
    es_rmse.pprint()

    es_rmse.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
          outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
          keyClass="org.apache.hadoop.io.NullWritable", 
          valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
          conf={"es.resource": "movies/rmse"}))

    # Recommendations - create list to add to 

    # recommendData = ratings.map(lambda r: (r[2],r[1])).transform(lambda rdd: rdd.join(movies.map(lambda r: (r[0], r[1]))))
    # recommendData.pprint()

    # movies.transform(lambda rdd: rdd.map(lambda r: r[0], r[1])).pprint()
    # recommendData = ratings.map(lambda r: (r[2],r[1])).transform(movieRec) #lambda rdd: rdd.join(movies.map(lambda r: (r[0], r[1]))))
    # recommendData.pprint()
	# preds = recommendData.map(lambda r: (r[0], r[1])).transform(lambda rdd: model.predictAll(rdd).map(lambda r: ('Recommendations',r[2]).sortByKey(lambda r: r[2])

	# recommendations = sorted(preds, key=lambda x: x[2], reverse=True)[:50]
 #    recommendations = sorted(preds.map(lambda r: ('key',r)), key=lambda x: x[2], reverse=True)[:1]
   
    # movies is an RDD of (movieId, movieTitle)
    # print ("Movies recommended for you:")
    # for i in xrange(len(recommendations)):
    # 	print ("%2d: %s" % (i + 1, movies[recommendations[i][1]])).encode('ascii', 'ignore')

    ssc.start()
    ssc.awaitTermination()