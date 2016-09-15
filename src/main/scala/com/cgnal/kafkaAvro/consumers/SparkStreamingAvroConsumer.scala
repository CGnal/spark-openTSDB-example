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

import java.io.ByteArrayInputStream

import com.cgnal.DataPoint
import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroType
import kafka.serializer.DefaultDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.util.Success

/**
 * Created by cgnal on 08/09/16.
 */
class SparkStreamingAvroConsumer {

  def run(implicit ssc: StreamingContext, topicsSet: Set[String], kafkaParams: Map[String, String]): DStream[DataPoint] = {

    println("in RUN!!")
    assert(kafkaParams.contains("metadata.broker.list"))

    val inputStream: InputDStream[(Array[Byte], Array[Byte])] = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)

    val lines: DStream[DataPoint] = inputStream.map { el =>
      val schema = AvroType[DataPoint]
      val bis = new ByteArrayInputStream(el._2)
      val io: AvroTypeIO[DataPoint] = schema.io
      val Success(dataPoint) = io.read(bis)
      dataPoint
    }

    lines

  }

  //  def run(ssc: StreamingContext, topicsSet: Set[String], kafkaParams: Map[String, String]):  Unit = {
  //
  //    assert(kafkaParams.contains("metadata.broker.list"))
  //
  //    val inputStream: InputDStream[(Array[Byte],Array[Byte])] = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
  //
  //    val lines: DStream[DataPoint] = inputStream.map{ el =>
  //      val schema = AvroType[DataPoint]
  //      val bis = new ByteArrayInputStream(el._2)
  //      val io: AvroTypeIO[DataPoint] = schema.io
  //      val Success(dataPoint) = io.read(bis)
  //      dataPoint
  //    }
  //
  //    lines.print(20)
  //  }

}

