import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.event.Level

import scala.collection.JavaConverters._

trait ConsumerHelper {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", classOf[KafkaExample.MapDeserializer[String, Long]])
  props.put("group.id", "mygroup")

  val TOPIC = "flink-destination"//"kafka-destination"
}

object ScalaKafkaConsumer extends App with ConsumerHelper {
  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while(true){
    val records=consumer.poll(100)
    for (record<-records.asScala){
      println(record)
    }
  }
}