# Kafka_Pyspark
Base Kafka Producer, consumer, flask api and PySpark Structured streaming Job


This repo contains a very basic example of kafka producer, consumer, flask api and a pyspark streamer, along with the required commands and installation steps




Install Kafka from  : https://kafka.apache.org/quickstart
Install CMAK : https://github.com/yahoo/CMAK

### Commands: 
Zookeeper: in kafka__ folder (in directory where you installed kafka) : **bin/zookeeper-server-start.sh config/zookeeper.properties**
Kafka Server: in kafka__ folder (in directory where you installed kafka) : **JMX_PORT=8004 bin/kafka-server-start.sh config/server.properties**
CMAK : in CMAK/target/universal/cmak__ folder : **./bin/cmak -Dconfig.file=conf/application.conf -Dhttp.port=8080**
Spark Structured Streaming Job: **spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<<Pyspark Version 3.1.2>> streamer.py**

