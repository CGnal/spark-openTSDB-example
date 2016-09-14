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

import java.util.Properties

import com.cgnal.kafkaAvro.consumers.SparkStreamingAvroConsumer
import com.cgnal.kafkaAvro.producer.KafkaAvroProducer
import com.cgnal.kafkaLocal.KafkaLocal
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

/**
  * Created by cgnal on 09/09/16.
  */
class SparkStreamingAvroConsumerSpec extends WordSpec with MustMatchers with BeforeAndAfterAll{
  var sparkContext: SparkContext = _
  var streamingContext: StreamingContext = _
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
    "bootstrap.servers" -> "localhost:9092")

  override def beforeAll(): Unit = {
    KafkaLocal.start()
    val conf = new SparkConf().
      setAppName("spark-opentsdb-example-test").
      setMaster("local[*]")
    sparkContext = new SparkContext(conf)
    streamingContext = new StreamingContext(sparkContext, Milliseconds(20))
  }

  "KafkaAvroProducer" must {
    "produce 200 data-points on topic test-spec" in {


      producer.run(2, 100, 0L, propsProducer, topic)

      //true must be equals true
    }

    "SparkStreamingAvroConsumer" must {
      "consume 200 data-point on topic test-spec" in {

        //val consumer : KafkaConsumer[Array[Byte], Array[Byte]]= new KafkaConsumer[Array[Byte], Array[Byte]](propsConsumer)
        //val topics = consumer.listTopics()
        //consumer.close()
        //topics.containsKey(topic) must be equals true

        consumer.run(streamingContext, Set(topic), propsConsumer)


      }
    }



  }




  override def afterAll(): Unit = {
    KafkaLocal.stop()
    streamingContext.stop(false)
    streamingContext.awaitTerminationOrTimeout(1000L)
    sparkContext.stop()
  }
}


