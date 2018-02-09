
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object TestRunningMultipleCalcs extends App {

  // used by 'time' method
//  implicit val baseTime = System.currentTimeMillis

  // 2 - create a Future
  val f = Future {
//    Thread.sleep(500)
    1 + 1
  }

  println("debug it!")
  // 3 - this is blocking (blocking is bad)
  val result = Await.result(f, 3 second)
  println(result)
//  Thread.sleep(1000)

}