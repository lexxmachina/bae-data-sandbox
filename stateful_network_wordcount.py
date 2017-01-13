from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: stateful_network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingStatefulNetworkWordCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint")

    zkQuorum, topic = sys.argv[1:]

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})  
    lines = kvs.map(lambda x: x[1])
    lines.pprint()

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    running_counts = lines.flatMap(lambda line: line.split(","))\
                          .map(lambda word: (word, 1))\
                          .updateStateByKey(updateFunc)

    running_counts.pprint()

    # running_counts.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
    #   outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
    #   keyClass="org.apache.hadoop.io.NullWritable", 
    #   valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    #   conf={"es.resource": "stream/recommendation"}))

    ssc.start()
    ssc.awaitTermination()