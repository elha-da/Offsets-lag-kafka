

import java.time.Instant
import java.util.concurrent.TimeUnit

import kafka.cluster.Broker
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.apache.zookeeper.ZooKeeper

import scala.collection.JavaConverters._

object ZooKeeperBroker extends App {
  val zookeeperConnect = "localhost:2181"

  val zkClient = ZkUtils.createZkClient(zookeeperConnect, 10000, 10000)
  println(s" create zookeeper connection : $zookeeperConnect")
  val zkUtils: ZkUtils = new ZkUtils(zkClient, zkConnection = new ZkConnection(zookeeperConnect), isSecure = false)

  val brokers: Seq[Broker] = zkUtils.getAllBrokersInCluster()

  zkUtils.close()
  println(s"zookeeper connection CLOSED")

  brokers.foreach {
    println("ici")
    b =>
      println(s"id : ${b.id} " +
        s"| endPoints : ${b.endPoints(0).host}:${b.endPoints(0).port} " +
        s"| ${System.currentTimeMillis} ${TimeUnit.MILLISECONDS}" +
        s"| ${Instant.now.getEpochSecond}")
      println()
  }

  //  val zk: ZooKeeper = new ZooKeeper("0.0.0.0:2181", 10000, null)
  //  val ids: List[String] = zk.getChildren("/brokers/ids", false).asScala.toList

  //  ids.foreach { id =>
  //    val brokerInfo: String = new String(zk.getData("/brokers/ids/" + id, false, null))
  //    println(brokerInfo)
  //  }
}
