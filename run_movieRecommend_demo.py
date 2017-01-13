# start services
echo "Starting services..."
sudo service elasticsearch restart
sudo service kibana restart
sudo service kafka-server restart

echo "setup elasticsearch indices"
python create_indices.py
#curl -XDELETE 'http://localhost:9200/metrics/'
#curl -XDELETE 'http://localhost:9200/customers/'

echo "Clear down kafka topic"
kafka-topics --zookeeper localhost:2181 --delete --topic gov
kafka-topics --zookeeper localhost:2181 --create --topic gov --partitions 1 --replication-factor 1

echo "To start spark streaming application run:"
echo "cd /root/spark/gov/"
echo "spark-submit --master local[2] --jars elasticsearch-hadoop-2.3.1.jar customers-streaming-app.py localhost:2181 gov"
echo ""
echo "run kafka producer in seperate shell"
echo "python kafka_csv_producer.py"
echo ""
echo "to check elasticsearch run.."
echo "curl localhost:9200/customers/customer/_search"
echo "curl localhost:9200/metrics/metric/_search"
 
