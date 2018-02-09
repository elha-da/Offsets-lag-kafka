
import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, KafkaConsumerGroupService}



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
                " LAG : " + partitionStates.lag)
      }
    }

    Thread.sleep(1000)
  }
}
