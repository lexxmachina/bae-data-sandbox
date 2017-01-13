

from kafka import SimpleProducer
from kafka import KafkaClient
import findspark
import os
findspark.init()

import pyspark
sc = pyspark.SparkContext()

from pyspark.sql.types import *
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

import json
import csv
kafka = KafkaClient("127.0.0.1:9092")
producer = SimpleProducer(kafka)

def kafkasend(message):
    return producer.send_messages('test2',str.encode(message))


with open('churn_small.csv', newline='') as csvfile:
         reader = csv.reader(csvfile, delimiter=',', quotechar='|')
         header= next(reader, None)
         for row in reader:
            #print(row)
            out = json.dumps(row)
            kafkasend(out)




