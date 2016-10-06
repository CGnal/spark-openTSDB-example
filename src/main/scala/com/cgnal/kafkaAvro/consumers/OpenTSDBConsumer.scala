package com.cgnal.kafkaAvro.consumers
import java.io.{ByteArrayInputStream, Serializable}

import com.cgnal.avro.Event
import com.cgnal.kafkaAvro.converters.{EventConverter, SimpleEventConverter}

import scala.collection.JavaConverters._
import com.cgnal.spark.opentsdb.{DataPoint, OpenTSDBContext}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.{GenericAvroCodecs, SpecificAvroCodecs}
import kafka.serializer.DefaultDecoder
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

import scala.reflect._
import scala.util.{Failure, Success}

/**
  * Created by cgnal on 20/09/16.
  */
class OpenTSDBConsumer[T <: EventConverter: ClassTag](@transient ssc: StreamingContext,
                       @transient hadoopConf: Configuration,
                       topicSet:Set[String],
                       kafkaParams: Map[String, String]
                      ) extends Serializable{

  val logger = LoggerFactory.getLogger(this.getClass)

  val openTSDBContext: Option[OpenTSDBContext] = initOpenTSDB()

  val converter: T = classTag[T].runtimeClass.newInstance().asInstanceOf[T]

  //val converter = ssc.sparkContext.broadcast(SpecificAvroCodecs.toBinary[Event].asInstanceOf[Injection[Event, Array[Byte]]with Serializable])

  private def initOpenTSDB() = {
    val sparkContext = ssc.sparkContext
    val sqlContext = new SQLContext(sparkContext)
    Some(new OpenTSDBContext(sqlContext, Some(hadoopConf)))
  }

  def this(@transient ssc: StreamingContext,
            @transient hadoopConf: Configuration,
            topicSet:Set[String],
            kafkaParams: Map[String, String],
            keytab:String,
            principal:String) = {
    this(ssc, hadoopConf, topicSet, kafkaParams)
    openTSDBContext.get.keytab = keytab
    openTSDBContext.get.principal = principal
    openTSDBContext.get.keytabLocalTempDir = "/tmp"
  }

  def run(): Unit = {

    assert(kafkaParams.contains("metadata.broker.list"))

    val inputStream: InputDStream[(Array[Byte], Array[Byte])] = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicSet)

    val stream = inputStream.mapPartitions{ rdd =>
      val specificAvroBinaryInjection: Injection[Event, Array[Byte]]= SpecificAvroCodecs.toBinary[Event]
      rdd.map{el =>
        val Success(event) = specificAvroBinaryInjection.invert(el._2)
        event
      }

    }.
      filter(converter.convert.isDefinedAt)
      .map(converter.convert)
     //stream.print(10)

    openTSDBContext.get.streamWrite(stream)
  }

}
