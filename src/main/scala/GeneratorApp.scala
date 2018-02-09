import java.util.Properties

import akka.actor.ActorSystem
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object GeneratorApp extends App {

  val properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val props = properties // .setProperty("num.partitions", "2")
  val props1 = properties

  val producer: KafkaProducer[String, String] = new KafkaProducer(props)
  val producer1 = new KafkaProducer[String, String](props1)

  var ctr = 1
  val system = ActorSystem("system")
  val firstInTopic = "topic-1"
  val secondInTopic = "topic-2"
  val thirdInTopic = "topic-3"

  system.scheduler.schedule(0 second, 200 milliseconds){
    val record1: ProducerRecord[String, String] = new ProducerRecord(firstInTopic, ctr.toString, "data from topic1 "+ctr)

    val record2_0: ProducerRecord[String, String] = new ProducerRecord(secondInTopic, 0, ctr.toString, "data from topic2-0 "+ctr)
    val record2_1: ProducerRecord[String, String] = new ProducerRecord(secondInTopic, 1, ctr.toString, "data from topic2-1 "+ctr/2)

//    val record3 = new ProducerRecord[String, String](thirdInTopic, ctr.toString, "data from topic3 "+ctr)
    ctr += 1
//    producer.send(record1)
    producer.send(record2_0)
    producer.send(record2_1)
//    producer1.send(record2)
//    producer.send(record3)

//    println(record1)
    println(record2_0)
    println(record2_1)
//    println(record3)
  }


}
