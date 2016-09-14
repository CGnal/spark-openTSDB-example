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

package com.cgnal.DataPointConsumer.sparkStreaming.examples

import com.cgnal.DataPointConsumer.sparkStreaming.SparkStreamingAvroConsumer
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cgnal on 09/09/16.
  */
object SparkStreamingAvroConsumerMain extends App {
  val sparkConf = new SparkConf().setAppName("kafkaStreaming-consumer").setMaster("local[*]")
  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

  val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
  val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.docker.brokers")
  val props = Map("metadata.broker.list" -> brokers)

  new SparkStreamingAvroConsumer().run(ssc, Set(topic), props)

  ssc.start()
  ssc.awaitTermination()


}