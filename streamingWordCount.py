from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import itertools
from math import sqrt

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("/t")
    return int(fields[0]), fields[1]

# predictAll takes 2 arguments: User and Product pairs
def predict_rdd(rdd):
	predict = model.predictAll(rdd).map(lambda r: (r[0], r[1], r[2]))
	return predict

def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndratingss = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndratingss.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sys.argv[0] <zk> <topic>", file=sys.stderr)
        sys.exit(1)

    sc = SparkContext(appName="StreamingKafkaConsumerSaveToElastic")

    ssc = StreamingContext(sc, 5)

    model = MatrixFactorizationModel.load(sc, "./target/tmp/movieLensRecom")

    zkQuorum, topic = sys.argv[1:]

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})  
    lines = kvs.map(lambda x: x[1])
    # lines.pprint()
  	
    ratings = lines.map(lambda line: line.split(","))
    ratings.cache()
    
    # counts = ratings.map(lambda x: (x[1], 1)).reducebyKey(lambda x, y: x+y)
    # Convert counts to format accepted by elastic converter
    es_ratings = ratings.map(lambda item: ('key', {
       'field':'wordcount',
       'val': item[0]
    }))

    # save results to elastic search
    es_ratings.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
      outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
      keyClass="org.apache.hadoop.io.NullWritable",
      valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
      conf={"es.resource": "stream/wordcounts"}))

    ssc.start()
    ssc.awaitTermination()