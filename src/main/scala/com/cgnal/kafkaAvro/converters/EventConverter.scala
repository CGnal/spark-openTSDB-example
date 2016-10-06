package com.cgnal.kafkaAvro.converters

import java.io.Serializable

import com.cgnal.avro.Event
import com.cgnal.spark.opentsdb.DataPoint
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs

/**
  * Created by cgnal on 06/10/16.
  */
trait EventConverter extends Serializable{

//  def simpleEventConverter: PartialFunction [Event, DataPoint[Double]] = {
//    case x  => //if isEvent(x) =>
//      val value = DataPoint[Double](x.getEventTypeId.toString, x.getTs, x.getEventTypeId.toDouble, x.getAttributes.asScala)
//      value.asInstanceOf[DataPoint[Double] with Serializable]
//  }

  //val specificAvroBinaryInjection: Injection[Event, Array[Byte]] = SpecificAvroCodecs.toBinary[Event]
  def convert: PartialFunction [Event, DataPoint[Double]]
}
