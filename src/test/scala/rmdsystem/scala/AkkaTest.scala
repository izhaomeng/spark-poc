package rmdsystem.scala

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import org.junit.Test
import akka.actor.ActorSystem
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import akka.dispatch.ExecutionContexts._
import scala.io.Source._

class AkkaTest {

  implicit val ec = global

  @Test
  def testAkka() {
    var textFile = "/home/cuikexi/git_workspace/rmdsystem/readme.md"

    val system = ActorSystem("System")
    val actor = system.actorOf(Props(new WordCounterActor(textFile)))
    implicit val timeout = Timeout(25 seconds)
    val future = actor ? StartProcessFileMsg()
    future.map { result =>
      println("Total number of words " + result)
      system.shutdown
    }

    Thread.sleep(3000)
  }

}

case class ProcessStringMsg(string: String)
case class StringProcessedMsg(words: Integer)
class StringCounterActor extends Actor {
  def receive = {
    case ProcessStringMsg(string) => {
      val wordsInLine = string.split(" ").length
      sender ! StringProcessedMsg(wordsInLine)
    }
    case _ => println("Error: message not recognized")
  }
}

case class StartProcessFileMsg()
class WordCounterActor(filename: String) extends Actor {

  private var running = false
  private var totalLines = 0
  private var linesProcessed = 0
  private var result = 0
  private var fileSender: Option[ActorRef] = None

  def receive = {
    case StartProcessFileMsg() => {
      if (running) {
        println("Warning: duplicate start message received")
      } else {
        running = true
        fileSender = Some(sender)
        fromFile(filename).getLines.foreach { line =>
          context.actorOf(Props[StringCounterActor]) ! ProcessStringMsg(line)
          totalLines += 1
        }
      }
    }
    case StringProcessedMsg(words) => {
      result += words
      linesProcessed += 1
      if (linesProcessed == totalLines) {
        fileSender.map(_ ! result) // provide result to process invoker  
      }
    }
    case _ => println("message not recognized!")
  }
}  