# To run this code:

## Run required servers in Local Mode
1. If you need kafka (and Zookeeper) run the class KafkaLocal setting the input parameter to "true" (the local instance of Kafka will be executed with Zookeeper)
2. If you need kafka and openTSDB then run the classes HbaseLocal and KafkaLocal setting the KafkaLocal's input parameter to "false" 

<!---
## Run with Dockers
## HBASE
As written in this (page) [docker pull everpeace/hbase-standalone], run
> docker pull everpeace/hbase-standalone
> docker run -d -p 2181:2181 -p 60000:60000 -p 60010:60010 -p 60020:60020 -p 60030:60030 everpeace/hbase-standalone

--->
## Generate the avro class
Download avro-tools from http://mvnrepository.com/artifact/org.apache.avro/avro-tools/1.8.1 and exec: 

> java -jar path/to/avro-tools-1.8.1.jar compile schema src/main/resources/Event.avsc ./src/main/scala


# Cloudera Manager Analysis
To check the number of messagges in kafka using Claudera Manager:

> ClauderaManager -> Kafka -> Chart Library -> Topics (click on topic to analyze)

To check the number of consumed messages in kafka using Claudera Manager:

> ClauderaManager -> Yarn -> WebUI -> ResourceManager WebUi -> ApplicationMaster -> Streaming


# Packaging 
To generate the jar with all dependencies in the ./lib directory:
> sbt universal:packageZipTarball
The tar will be found in path ./spark-opentsdb-examples/target/universal

To generate package without external dependencies:
> sbt clean package


# Run consumer in spark
```bash

  spark-submit --executor-memory 1200M \
  --jars $(JARS=("$(pwd)/lib"/*.jar); IFS=,; echo "${JARS[*]}") \
  --driver-class-path /etc/hbase/conf \
  --conf spark.executor.extraClassPath=/etc/hbase/conf \
  --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/tmp/jaas.conf \
  --conf "spark.driver.extraJavaOptions="-Dspark-opentsdb-exmaples.hbase.master=eligo105.eligotech.private:60000 -Dspark-opentsdb-exmaples.zookeeper.host=eligo105.eligotech.private:2181/kafka-Dspark-opentsdb-exmaples.kafka.brokers=192.168.2.108:9092" \
  --master yarn --deploy-mode client \
  --keytab flanotte.keytab \
  --principal flanotte@SERVER.ELIGOTECH.COM \
  --class com.cgnal.kafkaAvro.consumers.example.OpenTSDBConsumerMain spark-opentsdb-examples_2.10-1.0.0-SNAPSHOT.jar  false flanotte.keytab flanotte@SERVER.ELIGOTECH.COM
```  
  

# spark-cdh-template
A spark sbt template that you can use for bootstrapping your spark projetcs: http://www.davidgreco.me/blog/2015/04/11/a-spark-sbt-based-project-template/
http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
