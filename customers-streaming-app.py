from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def topCentileHousing(rdd):

  try:
    c = rdd.count()
    result = rdd.top(int((c/10))).pop()
    global currentTopCentileHousing
    currentTopCentileHousing = sc.broadcast(result)
  except:
    pass

def filterTopHousingCosts(rdd):
  
  if 'currentTopCentileHousing' in globals():
    return rdd.filter(lambda row: row['housingCosts'] > currentTopCentileHousing.value)
  else:
    return rdd.context.parallelize([])

def filterTopChildCareCosts(rdd):

  if 'currentTopCentileChildCare' in globals():
    return rdd.filter(lambda row: row['childCareCosts'] > currentTopCentileChildCare.value)
  else:
    return rdd.context.parallelize([])

def topCentileChildCare(rdd):

  try:
    c = rdd.count()
    result = rdd.top(int((c/10))).pop()
    global currentTopCentileChildCare
    currentTopCentileChildCare = sc.broadcast(result)
  except:
    pass

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sys.argv[0] <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="StreamingKafkaConsumerSaveToElastic")
    ssc = StreamingContext(sc, 1)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])

    records = lines.map(lambda line: line.split("@|"))

    named_records = records.map(lambda r: {'tstamp':r[0], 'firstname':r[1], 'surname':r[2],
      'dob':r[3], 'nino':r[4], 'housingCosts':r[5], 'childCareCosts':r[6], 'address':r[7], 'postcode':r[8]})

    named_records.cache()

    es_records = named_records.map(lambda r: ('key', r))
    es_records.pprint()
    
    es_records.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='_',
          outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", 
          keyClass="org.apache.hadoop.io.NullWritable", 
          valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
          conf={"es.resource": "customers/customer"}))

    hCosts = named_records.map(lambda r: r['housingCosts'])

    latestHousCosts = hCosts.window(10, 1)
    
    latestHousCosts.foreachRDD(topCentileHousing)

    topHousingRecords = named_records.transform(filterTopHousingCosts)

    topHousingDetails = topHousingRecords.map(lambda r: {'tstamp':r['tstamp'], 
                                    'nino':r['nino'],
                                    'risk_category': 'Top Centile Housing Costs', 
                                    'housingCosts':r['housingCosts']})

    esTopHousingDetails = topHousingDetails.map(lambda r: ('key',r))

    esTopHousingDetails.foreachRDD(lambda r: r.saveAsNewAPIHadoopFile(path='_',
          outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
          keyClass="org.apache.hadoop.io.NullWritable",
          valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
          conf={"es.resource": "risks/topCentileHousingCosts"}))

    cCosts = named_records.map(lambda r: r['childCareCosts'])

    latestChCosts = cCosts.window(10, 1)

    latestChCosts.foreachRDD(topCentileChildCare)

    topChildCareRecords = named_records.transform(filterTopChildCareCosts)

    topChildCareDetails = topChildCareRecords.map(lambda r: {'tstamp':r['tstamp'],
                                    'nino':r['nino'],
                                    'risk_category': 'Top Centile Childcare Costs',
                                    'childCareCosts':r['childCareCosts']})

    esTopChildCareDetails = topChildCareDetails.map(lambda r: ('key',r))

    esTopChildCareDetails.foreachRDD(lambda r: r.saveAsNewAPIHadoopFile(path='_',
          outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
          keyClass="org.apache.hadoop.io.NullWritable",
          valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
          conf={"es.resource": "risks/topCentileChildCareCosts"}))

    ssc.start()
    ssc.awaitTermination()
