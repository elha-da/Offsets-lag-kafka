
import java.util.{Calendar, Properties}
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import io.circe.{Encoder, Json}
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import kafka.utils.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random


object GeneratorApp extends App {

//  {"time":"2018-05-18T12:26:53Z","topic":"json-win","group":"events2delivery-k10-v6","lag":471}
  case class Lags(time: String, topic: String, group: String, lag: Long)

  implicit val lagTopicEncoder: Encoder[Lags] = new Encoder[Lags] {
    final def apply(tl: Lags): Json = Json.obj(
//      ("time", Json.fromLong(tl.time)),
      ("time", Json.fromString(tl.time)),
      ("topic", Json.fromString(tl.topic)),
      ("group", Json.fromString(tl.group)),
      ("lag", Json.fromLong(tl.lag))
    )
  }

//  {"time":"2018-05-18T12:26:53Z","broker":"10.19.23.69:9999","topic":"json-smaato","msgInRate":9}
  case class Metric(time: String, broker: String, topic: String, msgInRate: Long)

  implicit val metricTopicEncoder: Encoder[Metric] = new Encoder[Metric] {
    final def apply(tl: Metric): Json = Json.obj(
//      ("time", Json.fromLong(tl.time)),
      ("time", Json.fromString(tl.time)),
      ("broker", Json.fromString(tl.broker)),
      ("topic", Json.fromString(tl.topic)),
      ("msgInRate", Json.fromLong(tl.msgInRate))
    )
  }


  private val isoDateTimeFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'H:m:s'Z'")

  def getProperties(): Properties = {
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties
  }
  val properties = getProperties()

  val props = properties // .setProperty("num.partitions", "2")
  val props1 = properties

  val producer: KafkaProducer[String, String] = new KafkaProducer(props)
  val producer1 = new KafkaProducer[String, String](props1)

  val system = ActorSystem("system")
  val firstInTopic = "json-lags"
  val secondInTopic = "topic-2"
  val thirdInTopic = "topic-3"

  var ctr, ctrOld = 0
  var timeSec = Calendar.getInstance().get(Calendar.SECOND)
  system.scheduler.schedule(0 second, 1 seconds){ // 1 milliseconds){
//  while (true) {
    val currentTime: Long = Instant.now.getEpochSecond

    val record1
    : ProducerRecord[String, String] = new ProducerRecord(
      firstInTopic,
      null,
      currentTime,
      ctr.toString,
//      Lags(Instant.now.getEpochSecond, firstInTopic,  "group-59", Random.nextInt(250000)).asJson.noSpaces
      Lags(LocalDateTime.ofEpochSecond(1527069027, 0, ZoneOffset.UTC).format(isoDateTimeFormat), firstInTopic,  "group-59", Random.nextInt(250000)).asJson.noSpaces
//      s"{topic: topic1, lag: ${Random.nextInt(250000)}, time: ${LocalDateTime.ofEpochSecond(Instant.now.getEpochSecond, 0, ZoneOffset.UTC).format(isoDateTimeFormat)}}"
    )
    val record1_1
    : ProducerRecord[String, String] = new ProducerRecord(
      firstInTopic,
      null,
      currentTime,
      ctr.toString,
//      Lags(Instant.now.getEpochSecond, firstInTopic,  "group20", Random.nextInt(300000)).asJson.noSpaces
      Lags(LocalDateTime.ofEpochSecond(1527069303, 0, ZoneOffset.UTC).format(isoDateTimeFormat), firstInTopic,  "group20", Random.nextInt(300000)).asJson.noSpaces
//      s"{topic: topic1, lag: ${Random.nextInt(300000)}, time: ${LocalDateTime.ofEpochSecond(Instant.now.getEpochSecond, 0, ZoneOffset.UTC).format(isoDateTimeFormat)}}"
    )

    val record1_2
    : ProducerRecord[String, String] = new ProducerRecord(
      firstInTopic,
      null,
      currentTime,
      ctr.toString,
//      Metric(Instant.now.getEpochSecond,   "broker-12.226.0.20", "metric-topic", Random.nextInt(300000)).asJson.noSpaces
      Metric(LocalDateTime.ofEpochSecond(1527069399, 0, ZoneOffset.UTC).format(isoDateTimeFormat),   "broker-12.226.0.20", "metric-topic", Random.nextInt(300000)).asJson.noSpaces
//      s"{topic: topic1, lag: ${Random.nextInt(300000)}, time: ${LocalDateTime.ofEpochSecond(Instant.now.getEpochSecond, 0, ZoneOffset.UTC).format(isoDateTimeFormat)}}"
    )

//    val record2 : ProducerRecord[String, String] = new ProducerRecord(secondInTopic, ctr.toString, "data from topic2-0 "+ctr)
//    val record2_0: ProducerRecord[String, String] = new ProducerRecord(secondInTopic, 0, ctr.toString, "data from topic2-0 "+ctr)
//    val record2_1: ProducerRecord[String, String] = new ProducerRecord(secondInTopic, 1, ctr.toString, "data from topic2-1 "+ctr/2)

//    val record3  : ProducerRecord[String, String] = new ProducerRecord[String, String](thirdInTopic, ctr.toString, "data from topic3 "+ctr)

    producer.send(record1)
//    println(record1)

    producer.send(record1_1)
//    println(record1_1)

    producer.send(record1_2)
    println(record1_2)

//    producer.send(record3)
//    println(record3)

    if (timeSec != Calendar.getInstance().get(Calendar.SECOND)) {
      println(record1.value())
      println(record1_1.value())
      println(record1_2.value())
//      println(record3)
      println(s"number Msg Per Sec : ${ctr - ctrOld}")
//      Thread.sleep(3000)
      timeSec = Calendar.getInstance().get(Calendar.SECOND)
      ctrOld = ctr
    }
    ctr += 1

    //    producer.send(record2_0)
    //    producer.send(record2_1)
    //    println(record2_0)
    //    println(record2_1)
  }


}
