/*
 * Copyright 2016 CGnal S.p.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cgnal.kafkaAvro.producers.example

import java.io.File
import java.util.Properties

import com.cgnal.kafkaAvro.producers.KafkaAvroProducer
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory

/**
  * Created by cgnal on 08/09/16.
  */
object KafkaAvroProducerLocal {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    var config = ConfigFactory.load()

    args match {
      case Array(kafkaBrokers:String, zkHostIp:String) =>

       config = config
         .withValue("spark-opentsdb-exmaples.zookeeper.host",  ConfigValueFactory.fromAnyRef(zkHostIp))
         .withValue("spark-opentsdb-exmaples.kafka.brokers",   ConfigValueFactory.fromAnyRef(kafkaBrokers))

        logger.info("Changed default config in")
        logger.info(s"\t kafka: ${config.getString("spark-opentsdb-exmaples.kafka.brokers")}")
        logger.info(s"\t zookeeper: ${config.getString("spark-opentsdb-exmaples.zookeeper.host")}")

      case _ =>
    }

    val serializer = config.getString("spark-opentsdb-exmaples.kafka.serializer")
    val brokers = config.getString("spark-opentsdb-exmaples.kafka.brokers")
    val topic = config.getString("spark-opentsdb-exmaples.kafka.topic")
    val zookeepers = config.getString("spark-opentsdb-exmaples.zookeeper.host")

    val props = new Properties()

    //brokers are sequences of ip:port (e.g., "localhost:9092, 193.204.187.22:9092")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer)
    props.put("zookeeper.connect", zookeepers)

    new KafkaAvroProducer().run(3, 100, 30000L, props, topic)
  }


}
