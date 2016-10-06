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

package com.cgnal.kafkaAvro.consumers

import java.io.{ByteArrayInputStream, Serializable}

import akka.actor.TypedActor.SerializedMethodCall
import com.cgnal.avro.Event
import com.cgnal.kafkaAvro.converters.{EventConverter, SimpleEventConverter}

import scala.collection.JavaConverters._
import com.cgnal.spark.opentsdb.DataPoint
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.{GenericAvroCodecs, SpecificAvroCodecs}
import kafka.serializer.DefaultDecoder
import org.apache.avro.Schema
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

import scala.reflect._
import scala.util.{Success, Try}

/**
  * Created by cgnal on 08/09/16.
  */
class SparkStreamingAvroConsumer[T <: EventConverter: ClassTag](@transient ssc: StreamingContext,
                                                                topicSet:Set[String],
                                                                kafkaParams: Map[String, String]
                                                               ) extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)

  val converter= ssc.sparkContext.broadcast(classTag[T].runtimeClass.newInstance().asInstanceOf[T with Serializable])

  def run(): DStream[DataPoint[Double]] = {

    logger.info("Consumer is running")
    assert(kafkaParams.contains("metadata.broker.list"))


    val inputStream: InputDStream[(Array[Byte], Array[Byte])] = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicSet)

    inputStream.mapPartitions{ rdd =>
      val specificAvroBinaryInjection: Injection[Event, Array[Byte]]= SpecificAvroCodecs.toBinary[Event]
      rdd.map{el =>
        val Success(event) = specificAvroBinaryInjection.invert(el._2)
        event}
    }
      .filter(e => converter.value.convert. isDefinedAt(e))
      .map(converter.value.convert)
  }
}

