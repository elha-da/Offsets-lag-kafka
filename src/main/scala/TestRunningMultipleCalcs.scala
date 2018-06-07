
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object TestRunningMultipleCalcs extends App {

  // used by 'time' method
//  implicit val baseTime = System.currentTimeMillis

  // 2 - create a Future
  val f = Future {
    Thread.sleep(3000)
    1 + 1
  }

  f.onComplete({
    case Success(myInt) => {
      //Do something with my list
      println(myInt)
    }
    case Failure(ex) => {
      //Do something with my error
      println(ex)
    }
  })

  println("debug it!")

  // 3 - this is blocking (blocking is bad)
  val result = Await.result(f, 3 second)
  println(result)
  //  Thread.sleep(1000)

  println("debug it two !")


}