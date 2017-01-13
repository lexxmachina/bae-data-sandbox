from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="PythonStreamingNetworkWordCount")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost",9999)
counts = lines.flatMap(lambda line: line.split(" "))\
              .map(lambda word: (word, 1))\
              .reduceByKey(lambda a, b: a+b)
counts.pprint()

esCounts = counts.map(lambda r : ('key', r))

esCounts.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
  outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
  keyClass="org.apache.hadoop.io.NullWritable", 
  valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
  conf={"es.resource": "stream/recommendation"}))

ssc.start()
ssc.awaitTermination()