package nodescala

import scala.language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import nodescala.NodeScala.{Request, Response}

object Main {

  def main(args: Array[String]) {
    // TO IMPLEMENT
    // 1. instantiate the server at 8191, relative path "/test",
    //    and have the response return headers of the request
    val myServer = new NodeScala.Default(8191)
    def handler(request: Request): Response = {
      def m(p: (String, List[String])): String = s"key: ${p._1}, value: " + p._2.mkString("[", ",", "]")
      request.toList.map(m).iterator
    }
    val myServerSubscription = myServer.start("/test")(handler)

    // TO IMPLEMENT
    // 2. create a future that expects some user input `x`
    //    and continues with a `"You entered... " + x` message
    val userInterrupted: Future[String] = {
      var s: String = ""
      def cw(f: Future[String]): String = {
        val res = Await.result(f, Duration.Inf)
        s"You entered... $res"
      }
      Future.userInput(s).continueWith(cw)
    }

    // TO IMPLEMENT
    // 3. create a future that completes after 20 seconds
    //    and continues with a `"Server timeout!"` message
    val timeOut: Future[String] = {
      def cont(f: Future[Unit]): String = "Server timeout!"
      Future.delay(20 seconds).continueWith(cont)
    }

    // TO IMPLEMENT
    // 4. create a future that completes when either 10 seconds elapse
    //    or the user enters some text and presses ENTER
    val terminationRequested: Future[String] = Future.any(List(userInterrupted, timeOut))

    // TO IMPLEMENT
    // 5. unsubscribe from the server
    terminationRequested onSuccess {
      case msg =>
        println(s"Message reads: $msg")
        myServerSubscription.unsubscribe()
        println("Bye!")
    }
  }
}
