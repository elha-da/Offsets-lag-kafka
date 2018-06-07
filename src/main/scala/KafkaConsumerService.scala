
import java.sql.Timestamp
import java.util
import java.util.{Calendar, Properties}
import javax.management.{Attribute, InstanceNotFoundException, MBeanServerConnection, ObjectName}
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import io.circe.Json
import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, KafkaConsumerGroupService}
import kafka.server._
import kafka.metrics.{KafkaMetricsConfig, KafkaMetricsGroup, KafkaMetricsReporter}

import scala.io.Source
import io.circe._
import io.circe.syntax._
import io.circe.parser.parse
import kafka.tools.JmxTool
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.metrics._
import org.apache.zookeeper.ZooKeeper
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}


case class GroupInfo(state: String, partitionAssignmentStates: Seq[PartitionAssignmentState] = Seq())

case class PartitionAssignmentState(group: String,
//                                    coordinator: Option[Node],
                                    topic: String,
                                    partition: Int,
                                    offset: Long,
                                    lag: Long,
                                    consumerId: String,
                                    host: String,
                                    clientId: String,
                                    logEndOffset: Long)

//case class Node(id: Option[Int] = None,
//                idString: Option[String] = None,
//                host: Option[String] = None,
//                port: Option[Int] = None,
//                rack: Option[String] = None)

case class TopicsLagLimit (topic: String, lagLimit: Int)

object KafkaConsumerService extends App {

  /*def createKafkaConsumerGroupService(baseConfig: Array[String]): KafkaConsumerGroupService = {
    new KafkaConsumerGroupService(new ConsumerGroupCommandOptions(baseConfig))
  }

  def createKafkaConsumerGroupService(groupId: String = null): ConsumerGroupCommand.KafkaConsumerGroupService = {
    val kafkaAdress = "localhost:9092"

    val baseConfig: Array[String] = Array("--bootstrap-server", kafkaAdress)

    groupId match {
      case null => createKafkaConsumerGroupService(baseConfig)
      case _ => createKafkaConsumerGroupService(baseConfig ++ Array("--group", groupId))
    }
  }*/

  def list(): List[String] = {
    val baseConfig: Array[String] = Array("--bootstrap-server", "localhost:9092")
    val adminClient = new KafkaConsumerGroupService(new ConsumerGroupCommandOptions(baseConfig))

    try {
      adminClient.listGroups()
    } finally {
      adminClient.close()
    }
  }

  def describeConsumerGroup(group: String): GroupInfo = {
    val baseConfig: Array[String] = Array("--bootstrap-server", "localhost:9092")++ Array("--group", group)
    val kafkaConsumerGroupService = new KafkaConsumerGroupService(new ConsumerGroupCommandOptions(baseConfig))

    try {
      val (state, assignments) = kafkaConsumerGroupService.describeGroup()

      assignments match {
        case Some(partitionAssignmentStates) =>
          GroupInfo(state.getOrElse(""),
          partitionAssignmentStates.map(a => PartitionAssignmentState(
          a.group,
          //                      a.coordinator match {
          //                        case Some(c) => Some(Node(Option(c.id), Option(c.idString), Option(c.host), Option(c.port), Option(c.rack)))
          //                        case None => None
          //                      },
          a.topic.getOrElse(""),
          a.partition.getOrElse(0),
          a.offset.getOrElse(0),
          a.lag.getOrElse(0),
          a.consumerId.getOrElse(""),
          a.host.getOrElse(""),
          a.clientId.getOrElse(""),
          a.logEndOffset.getOrElse(0)
      )))
        case None => GroupInfo(state.getOrElse(""))
      }
    } finally {
      kafkaConsumerGroupService.close()
    }
  }


  //  case class TopicsLagLimit (topic: String, lagLimit: Int)

  //  println(s"${System.getProperty("user.dir")}")
  val source: String = Source.fromFile("jsons/topicsLag.json").getLines.mkString
  //  println(source)

  val jsonDoc: Json = parse(source).getOrElse(Json.Null)
//  println(jsonDoc)


  val topics = jsonDoc.hcursor
    .downField("topics-lag-limit-list")
    .focus
    .flatMap(_.asArray)
    .getOrElse(Vector.empty)


  implicit val decodeTopicsLagLimit: Decoder[TopicsLagLimit] =
    new Decoder[TopicsLagLimit] {
      final def apply(c: HCursor): Decoder.Result[TopicsLagLimit] =
      for {
         topic <- c.downField("topic").as[String]
         lagLimit <- c.downField("lagLimit").as[Int]
      } yield {
        new TopicsLagLimit(topic, lagLimit)
      }
    }

  val topicsLagLimitConfig =  topics.map(t => t.as[TopicsLagLimit].getOrElse(null)).map(o => o.topic -> o.lagLimit).toMap
  println(topicsLagLimitConfig)



//  val topicsLagLimit = decode[jsonDoc](decodeTopicsLagLimit)

//  println(topicsLagLimit)




  while (true) {

//    println(s"Received request for consumer list")
    val consumerGroupServiceList = list()
//    println(s"List consumer group : $consumerGroupServiceList")

    for (consumerGroupName <- consumerGroupServiceList) {
      println(s"Received request for : $consumerGroupName")
      val groupInfo = describeConsumerGroup(consumerGroupName)
//      println(s"$consumerGroupName : $groupInfo"+groupInfo)

      for (partitionStates <- groupInfo.partitionAssignmentStates) {
        println("groupId : "+ partitionStates.group +
                ", statute : "+ groupInfo.state +
                ", Topic : "+ partitionStates.topic +
                ", Partition : "+ partitionStates.partition +
                ", Offset : "+ partitionStates.offset +
                ", logEndOffset : "+ partitionStates.logEndOffset +
                " LAG : " + partitionStates.lag)

//        val jmxTool = JmxTool.main(Array("--attributes", "OneMinuteRate, Count, FifteenMinuteRate, FiveMinuteRate, MeanRate",
//          "--object-name", "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=topic-1"))

      }
    }

/* *********************************************************** */

    import kafka.admin.TopicCommand._

    val zookeeperConnect = "localhost:2181"
    val sessionTimeoutMs = 10 * 1000
    val connectionTimeoutMs = 8 * 1000

    val zkClient = ZkUtils.createZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs)
    val zkUtils = new ZkUtils(zkClient, zkConnection = new ZkConnection(zookeeperConnect), isSecure = false)

    val topicsList = zkUtils.getAllTopics().map(e => e -> e).toMap
//    val topicsList = listTopics(zkUtils, new TopicCommandOptions(Array("--list")))
    println(s"topicsList : $topicsList")

    case class MeterMetric(count: Long,
                       fifteenMinuteRate: Long,
                       fiveMinuteRate: Long,
                       oneMinuteRate: Long,
                       meanRate: Long)

    def getMeterMetric(mbsc: MBeanServerConnection, name: ObjectName) = {
      import scala.collection.JavaConverters._
      try {
        val attributesArray: Array[String] = mbsc.getMBeanInfo(name).getAttributes.map(attributeInfo => attributeInfo.getName)

//        val attributeList = mbsc.getAttributes(name, Array("Count", "FifteenMinuteRate", "FiveMinuteRate", "OneMinuteRate", "MeanRate"))
        val attributeList = mbsc.getAttributes(name, attributesArray)
        val attributes = attributeList.asList().asScala.toSeq

        MeterMetric(
          getLongValue(attributes, "Count"),
          getDoubleValue(attributes, "FifteenMinuteRate").round,
          getDoubleValue(attributes, "FiveMinuteRate").round,
          getDoubleValue(attributes, "OneMinuteRate").round,
          getDoubleValue(attributes, "MeanRate").round
        )
      } catch {
        case _: InstanceNotFoundException => MeterMetric(0,0,0,0,0)
      }
    }

    def getLongValue(attributes: Seq[Attribute], name: String) = {
      attributes.find(_.getName == name).map(_.getValue.asInstanceOf[Long]).getOrElse(0L)
    }

    def getDoubleValue(attributes: Seq[Attribute], name: String) = {
      attributes.find(_.getName == name).map(_.getValue.asInstanceOf[Double]).getOrElse(0D)
    }

//    val (jmxHost, jmxPort) = ("localhost","9999")
//    val urlString = s"service:jmx:rmi:///jndi/rmi://$jmxHost:$jmxPort/jmxrmi"
    val urlString = s"service:jmx:rmi:///jndi/rmi://:9999/jmxrmi"
    val url = new JMXServiceURL(urlString)
    val connector = JMXConnectorFactory.connect(url, null)
    val mbsc = connector.getMBeanServerConnection

    import scala.collection.JavaConverters._

    val zk: ZooKeeper = new ZooKeeper("0.0.0.0:2181", 10000, null)
    val allTopicsBroker = zk.getChildren("/brokers/topics", false).asScala.toList.map(e => e -> e).toMap

    println(s"ZooKeeper all topics Broker ${allTopicsBroker}")

    val topicswithLag = Map(1 -> "topic-1", 2 -> "topic-3")
    println(s"topics with Lag ${topicswithLag}")


    val otherTopics = allTopicsBroker --(topicswithLag.map(_._2))

    println(s"other topics ${otherTopics}")

    for ((idTopic, topic) <- allTopicsBroker) {
      //      val topic = "topic-1"
      val metricName = "MessagesInPerSec"

      val objectName = new ObjectName(s"kafka.server:type=BrokerTopicMetrics,name=$metricName,topic=$topic")

      val meterMetricMsgInPerSec: MeterMetric = getMeterMetric(mbsc, objectName)

      val now = Calendar.getInstance() //.getTime()
      println(s" ************************************************************ ")
      //    println(s" *************** ${Timestamp.valueOf("yyyy-MM-dd hh:mm:ss.fffffffff")} Timestamp " )
      println(s" **************** ${now.get(Calendar.MINUTE)} min " +
        s"${now.get(Calendar.SECOND)} sec " +
        s"${now.get(Calendar.MILLISECOND)} millisec **************** ")
      println(s" ************************************************************ ")

      //    val logEndOffsetList: Map[String, Long] = Map("old" -> logEndOffsetOld, "current" -> meterMetricMsgInPerSec.count)
      //    val logEndOffsetOld =  meterMetricMsgInPerSec.count
      println(s"----> $topic messages In Per Sec \n" +
        s"- count : ${meterMetricMsgInPerSec.count} \n" +
        s"- meanRate : ${meterMetricMsgInPerSec.meanRate} \n" +
        s"- FiveMinuteRate : ${meterMetricMsgInPerSec.fiveMinuteRate} \n" +
        s"- oneMinuteRate : ${meterMetricMsgInPerSec.oneMinuteRate} \n")
      //      s"- oneMinuteRate : ${logEndOffsetList("current") - logEndOffsetList("old")} \n")


      implicit val metricsTopicEncoder: Encoder[MeterMetric] = deriveEncoder

      val value = meterMetricMsgInPerSec.asJson.noSpaces
      val record: ProducerRecord[String, String] = new ProducerRecord("topic-3", value)

      val properties = GeneratorApp.getProperties()

      val producer: KafkaProducer[String, String] = new KafkaProducer(properties)
      producer.send(record)
      println(record)

    }
    Thread.sleep(5000)
  }
}
