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

package com.cgnal.DataPointConsumer.sparkStreaming

import java.io.File
import java.nio.file.Files
import java.util.Properties
import java.util.logging.LogManager

import com.cgnal.DataPoint
import com.cgnal.kafkaAvro.consumers.SparkStreamingAvroConsumer
import com.cgnal.kafkaAvro.producers.KafkaAvroProducer
import com.cgnal.kafkaLocal.KafkaLocal
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest._
import org.apache.spark.util.ManualClock

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by cgnal on 09/09/16.
 */
class KafkaAvroSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  var sparkContext: SparkContext = _
  implicit var streamingContext: StreamingContext = _
  val producer = new KafkaAvroProducer()
  val consumer = new SparkStreamingAvroConsumer()

  val topic = "test-spec"
  val propsProducer = new Properties()
  propsProducer.put("bootstrap.servers", "localhost:9092")
  propsProducer.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  propsProducer.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

  val propsConsumer = Map(
    "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
    "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
    "bootstrap.servers" -> "localhost:9092",
    "metadata.broker.list" -> "localhost:9092"
  )

  override def beforeAll(): Unit = {

    val dataDirectory = System.getProperty("java.io.tmpdir")
    val dir = new File(dataDirectory, "hadoop")
    dir.deleteOnExit()
    System.setProperty("hadoop.home.dir", dir.getAbsolutePath)
    // KafkaLocal.start()
    //KafkaLocal.createTopic(topic)
    val conf = new SparkConf().
      setAppName("spark-opentsdb-example-test").
      setMaster("local[*]")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    sparkContext = new SparkContext(conf)
    streamingContext = new StreamingContext(sparkContext, Milliseconds(500))
  }

  "KafkaAvroSpec" must {
    "produce 200 data-points on topic test-spec" in {

      producer.run(2, 100, 0L, propsProducer, topic)

      val propsTestConsumer = new Properties()
      propsTestConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      propsTestConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      propsTestConsumer.put("bootstrap.servers", "localhost:9092")
      propsTestConsumer.put("group.id", "consumer-test")
      propsTestConsumer.put("zookeeper.connect", "localhost:2181")
      propsTestConsumer.put("zookeeper.session.timeout.ms", "400")
      propsTestConsumer.put("zookeeper.sync.time.ms", "200")
      propsTestConsumer.put("auto.commit.interval.ms", "1000")

      val testConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = new KafkaConsumer[Array[Byte], Array[Byte]](propsTestConsumer)
      val topics = testConsumer.listTopics()
      val data = testConsumer.poll(100)
      data.iterator().toList.size must be equals 200
      topics.containsKey(topic) must be equals true
      testConsumer.close()
    }

    "consume 200 data-point on topic test-spec" in {

      var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[DataPoint]]
      val thread = new Thread(new Runnable {
        override def run(): Unit = producer.run(2, 100, 500, propsProducer, topic)
      })
      thread.start()

      val stream: DStream[DataPoint] = consumer.run(streamingContext, Set(topic), propsConsumer)
      stream.foreachRDD { rdd =>
        resultsRDD += rdd.collect()
        ()
      }
      streamingContext.start()
      Thread.sleep(2000)
      streamingContext.stop(false, false)

      val resultArrayFromRDD = resultsRDD.flatten.toList

      resultArrayFromRDD.size must be equals 200

    }
  }

  override def afterAll(): Unit = {
    KafkaLocal.stop()
    sparkContext.stop()
  }
}

