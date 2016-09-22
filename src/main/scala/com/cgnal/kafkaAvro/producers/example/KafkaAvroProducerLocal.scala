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
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * Created by cgnal on 08/09/16.
  */
object KafkaAvroProducerLocal extends App {
  //val classLoader = this.getClass.getClassLoader
  //val file = new File(classLoader.getResource("application.conf").getFile())

  val serializer = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.serializer")
  val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.docker.brokers")
  val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
  val props = new Properties()

  //brokers are sequences of ip:port (e.g., "localhost:9092, 193.204.187.22:9092")
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer)

  new KafkaAvroProducer().run(3, 100, 30000L, props, topic)
}
