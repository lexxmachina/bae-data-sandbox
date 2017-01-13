import sys
import csv
import datetime
import time
import ConfigParser
from kafka import KafkaProducer
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read('input_customer.properties')
kafka_server=parser.get('tool_config','kafka_server')
producer = KafkaProducer(bootstrap_servers=kafka_server)
linecount = 0
speedup  = int(parser.get('tool_config','speedup'))
input_file = parser.get('tool_config','input_file')
delimiter =  parser.get('tool_config','delimiter')
quotechar =  parser.get('tool_config','quotechar')
dateformatstr = parser.get('tool_config','dateformat')
topic =  parser.get('tool_config','topic')
additional_microseconds = parser.get('tool_config','additional_microseconds')
output_delimiter = parser.get('tool_config','output_delimiter')
with open(input_file,'rb') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        for row in csvreader:
                linecount=linecount+1
                if linecount > 1:
                        newdate = datetime.datetime.strptime(row[0]+additional_microseconds,dateformatstr)
                        if linecount == 2:
                                prevdate = newdate
                                date = newdate
                        else :
                                prevdate = date
                        date = newdate
                        diff = newdate - prevdate
                        sleeptime = diff.seconds/speedup
                        time.sleep(sleeptime)
                print 'sending message...'
                producer.send(topic,b''+output_delimiter.join(row))
producer.flush()
