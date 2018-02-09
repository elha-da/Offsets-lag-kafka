import java.{lang, util}
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._


object Consumer extends App {

  def props(groupIdConfig: String): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig)

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    return properties
  }

  val pr = props("KafkaExampleConsumer0000")
  val pr1 = props("KafkaExampleConsumer0001")
  val pr2 = props("KafkaExampleConsumer0002")

  val consumer = new KafkaConsumer[String, String](pr)
  val consumer1 = new KafkaConsumer[String, String](pr1)
  val consumer2 = new KafkaConsumer[String, String](pr2)


  def consuming(consumer: KafkaConsumer[String, String], topic: String, partition: Int) {
  //  consumer.subscribe(util.Arrays.asList(topic))

    val partition0: TopicPartition = new TopicPartition(topic, partition)
    consumer.assign(util.Arrays.asList(partition0))

    val records: ConsumerRecords[String, String] = consumer.poll(1000)


    val topicPartition = consumer.assignment()
    println(topicPartition)

    consumer.commitSync()

    val endOffsetsPartitionMap: util.Map[TopicPartition, lang.Long] = consumer.endOffsets(topicPartition)
    val logEndOffset: Long = endOffsetsPartitionMap.get(topicPartition.head)

    val recordsList = records.asScala.toList
    val lastReadOffset: Long = if (!records.isEmpty) (recordsList.last.offset()+1) else 0

    val consumerLag = logEndOffset - lastReadOffset



    println("consumer: "+ consumer)
    println("CURRENT-OFFSET: "+ lastReadOffset)
    println("LOG-END-OFFSET: "+ logEndOffset)
    println("LAG           : "+ consumerLag)

  }

  while (true) {
    consuming(consumer1, "topic-2", 0)
//    consuming(consumer1, "topic-2", 1)
//    consuming(consumer1, "topic-1")
//    consuming(consumer2, "topic-2")
    Thread.sleep(10000)
  }
//  consuming(consumer, "topic-1")
//  consuming(consumer, "topic-2")
//  consuming(consumer3, "topic-3")

}


