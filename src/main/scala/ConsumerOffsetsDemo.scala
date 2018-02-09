import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import org.apache.log4j.{Level, Logger}

object ConsumerOffsetsDemo extends App {

//  Logger.getRootLogger.setLevel(Level.OFF)

  def props(groupIdConfig: String): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig)
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    return properties
  }


  def consuming(consumer: KafkaConsumer[String, String], topic: String) {
    consumer.subscribe(util.Arrays.asList(topic))

    println(consumer)

    val recordsFromConsumer = consumer.poll(100)
    val topicPartition = consumer.assignment()

    val endOffsetsPartitionMap = consumer.endOffsets(topicPartition)
    println(s"endOffsetsPartitionMap : $endOffsetsPartitionMap")
    //  val currentPosition = consumer.position(partitionsAssigned.toList.head)
    val logEndOffset: Long = if (!endOffsetsPartitionMap.isEmpty) endOffsetsPartitionMap.get(topicPartition.head) else 0

    val recordsFromConsumerList = recordsFromConsumer.asScala.toList
    val lastReadOffset: Long = if (!recordsFromConsumer.isEmpty) recordsFromConsumerList.last.offset() else logEndOffset

    val consumerLag = logEndOffset - lastReadOffset

//    println(s"current Position : ${currentPosition}")
//    println(s"last Read Offset : ${lastReadOffset}")
//    println(s"log End Offset : ${logEndOffset}")
    println(s"Consumer Lag : ${consumerLag}")
  }

//    val pr1 = props("KafkaExampleConsumer")
  val pr1 = props("referential-stream")
  val consumer = new KafkaConsumer[String, String](pr1)

  while (true) {
    consuming(consumer, "topic-2")
    Thread.sleep(10)
  }

}
