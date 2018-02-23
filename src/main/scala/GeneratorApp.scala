
import java.util.{Calendar, Properties}

import akka.actor.ActorSystem
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object GeneratorApp extends App {

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
  val firstInTopic = "topic-1"
  val secondInTopic = "topic-2"
  val thirdInTopic = "topic-3"

  var ctr, ctrOld = 0
  var timeSec = Calendar.getInstance().get(Calendar.SECOND)
  system.scheduler.schedule(0 second, 1 milliseconds){
//  while (true) {
    val record1  : ProducerRecord[String, String] = new ProducerRecord(firstInTopic, ctr.toString, "data from topic1 "+ctr+" : "+ Calendar.getInstance().get(Calendar.SECOND))
    val record1_1: ProducerRecord[String, String] = new ProducerRecord(firstInTopic, ctr.toString, "data from topic1 "+(ctr-1)+" : "+ Calendar.getInstance().get(Calendar.SECOND))

    val record2 : ProducerRecord[String, String] = new ProducerRecord(secondInTopic, ctr.toString, "data from topic2-0 "+ctr)
//    val record2_0: ProducerRecord[String, String] = new ProducerRecord(secondInTopic, 0, ctr.toString, "data from topic2-0 "+ctr)
//    val record2_1: ProducerRecord[String, String] = new ProducerRecord(secondInTopic, 1, ctr.toString, "data from topic2-1 "+ctr/2)

//    val record3  : ProducerRecord[String, String] = new ProducerRecord[String, String](thirdInTopic, ctr.toString, "data from topic3 "+ctr)

    producer.send(record1)
//    println(record1)

    producer.send(record1_1)
//    println(record1_1)

    producer.send(record2)
//    println(record2)

//    producer.send(record3)
//    println(record3)

    if (timeSec != Calendar.getInstance().get(Calendar.SECOND)) {
      println(record1)
      println(record1_1)
      println(record2)
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
