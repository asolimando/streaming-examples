package kafka

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeUtils, Duration}

trait ProducerHelper {
  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val TOPIC = "origin"
  val CSV_PATH = "data/01.01.2016.csv"

  val SEPARATOR = ","
  val DATETIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
}

object ScalaKafkaProducer extends App with ProducerHelper {

  val producer = new KafkaProducer[String, String](props)
  val bufferedSource = scala.io.Source.fromFile(CSV_PATH)
  val sourceIterator = bufferedSource.getLines

  val KEY_INDEX = 0
  val TIMESTAMP_INDEX = 3

  // skip header
  sourceIterator.next

  // send the first event immediately, from then "respect" intra-events time
  val firstLine = sourceIterator.next
  val cols = firstLine.split(SEPARATOR).map(_.trim)
  val record = new ProducerRecord(TOPIC, cols(KEY_INDEX), firstLine)

  producer.send(record)

  var lastSendTimestamp = DateTime.now
  var lastEventTimestamp = DATETIME_FORMATTER.parseDateTime(cols(TIMESTAMP_INDEX))

  for (line <- sourceIterator) {
    val cols = line.split(SEPARATOR).map(_.trim)
    val record = new ProducerRecord(TOPIC, cols(KEY_INDEX), line)

    val actualEventTimestamp = DATETIME_FORMATTER.parseDateTime(cols(TIMESTAMP_INDEX))

    val intraEventDuration = DateTimeUtils.getDurationMillis(
      new Duration(lastEventTimestamp, actualEventTimestamp)
    )

    val nextSendTimestamp = lastSendTimestamp.plus(intraEventDuration)

    var timeElapsedLastSend = Math.max(
      DateTimeUtils.getDurationMillis(
        new Duration(lastSendTimestamp, nextSendTimestamp)
      ),
      0
    )

    Thread.sleep(timeElapsedLastSend)

    producer.send(record)

    lastEventTimestamp = actualEventTimestamp
    lastSendTimestamp = DateTime.now

    println(lastSendTimestamp + " -> " + line)
  }

  bufferedSource.close
  producer.close
}