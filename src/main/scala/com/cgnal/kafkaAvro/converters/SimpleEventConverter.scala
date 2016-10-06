package com.cgnal.kafkaAvro.converters
import com.cgnal.avro.Event
import com.cgnal.spark.opentsdb.DataPoint
import scala.collection.JavaConversions._


/**
  * Created by cgnal on 06/10/16.
  */
class SimpleEventConverter extends EventConverter{

  override def convert: PartialFunction[Event, DataPoint[Double]] = {
  case event:Event if true =>
    val javaMap = event.getAttributes
    //a conversion from avro.UTF8 to string is required to avoid exceptions
    val convertedMap = javaMap.map{case (k,v) => (k.toString,v.toString)}.toMap
    DataPoint[Double](metric = event.getEventTypeId.toString,
        timestamp = event.getTs,
        value = -1.toDouble,
        tags = convertedMap)
}
}
