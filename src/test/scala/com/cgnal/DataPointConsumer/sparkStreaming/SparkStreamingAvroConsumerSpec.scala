package com.cgnal.DataPointConsumer.sparkStreaming

import java.util.Properties

import com.cgnal.DataPointProducer.kafka.KafkaAvroProducer
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

/**
  * Created by cgnal on 09/09/16.
  */
class SparkStreamingAvroConsumerSpec extends WordSpec with MustMatchers with BeforeAndAfterAll{
  var sparkContext: SparkContext = _
  var streamingContext: StreamingContext = _
  //val consumer: SparkStreamingAvroConsumer = _
  val producer = new KafkaAvroProducer()


  override def beforeAll(): Unit = {
    val conf = new SparkConf().
      setAppName("spark-opentsdb-example-test").
      setMaster("local[*]")
     sparkContext = new SparkContext(conf)
     streamingContext = new StreamingContext(sparkContext, Milliseconds(20))
  }

  "KafkaAvroProducer" must {
    "write some data-point on kafka" in {

      val topic = "test-spec"
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

      producer.run(2, 100, 30000L, props, topic)
    }



  }




  override def afterAll(): Unit = {
    streamingContext.stop(false)
    streamingContext.awaitTermination()
    sparkContext.stop()
  }
}


