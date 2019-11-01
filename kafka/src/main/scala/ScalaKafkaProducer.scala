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

  val TOPIC_RAWDATA = "origin"
  val CSV_PATH = "data/01.01.2016.csv"

  val SEPARATOR = ","
  val DATETIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  val TOPIC_LOC = "locations"

  val locations = Seq(
    "1,124.7",
    "2,74.6",
    "3,124.7",
    "4,46.5",
    "5,106.1",
    "8,117.8",
    "9,31.8",
    "10,54.5",
    "12,113.8",
    "13,134.5",
    "15,98.4",
    "16,60.5",
    "18,66.8",
    "20,88.4",
    "21,20.8",
    "22,135.3",
    "23,80.8",
    "25,106.1",
    "26,48.5",
    "27,22.6",
    "28,31.8"
  )
}

object ScalaKafkaProducer extends App with ProducerHelper {

  val producer = new KafkaProducer[String, String](props)

  // create the topic for locations
  seqToTopic(locations, producer, TOPIC_LOC, 0)

  val bufferedSource = scala.io.Source.fromFile(CSV_PATH)
  val sourceIterator = bufferedSource.getLines

  val KEY_INDEX = 0
  val TIMESTAMP_INDEX = 3

  // skip header
  sourceIterator.next

  // send the first event immediately, from then "respect" intra-events time
  val firstLine = sourceIterator.next
  val cols = firstLine.split(SEPARATOR).map(_.trim)
  val record = new ProducerRecord(TOPIC_RAWDATA, cols(KEY_INDEX), firstLine)

  producer.send(record)

  var lastSendTimestamp = DateTime.now
  var lastEventTimestamp = DATETIME_FORMATTER.parseDateTime(cols(TIMESTAMP_INDEX))

  for (line <- sourceIterator) {
    val cols = line.split(SEPARATOR).map(_.trim)
    val record = new ProducerRecord(TOPIC_RAWDATA, cols(KEY_INDEX), line)

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
  producer.close()

  def seqToTopic(data: Seq[String],
                 producer: KafkaProducer[String, String],
                 topic: String,
                 keyIdx: Int): Unit =
    data.map(_.split(SEPARATOR))
        .map(l => (l(keyIdx), l.mkString(SEPARATOR)))
        .map(e => new ProducerRecord(topic, e._1, e._2))
        .foreach(producer.send)
}