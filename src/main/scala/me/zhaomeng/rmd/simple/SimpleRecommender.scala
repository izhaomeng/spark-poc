package me.zhaomeng.rmd.simple

import org.apache.spark.SparkContext
import play.api.libs.json._
import play.api.libs.functional.syntax._

class SimpleRecommender(val master: String, val appName: String, val dataDir: String) {
  val sc: SparkContext = new SparkContext(master, appName)

  def this(master: String, appName: String) {
    this(master, appName, "data")
  }

  def this(url: String) {
    this(url, "SimpleRecommender")
  }

  def this() {
    this("local[*]")
  }

  def queryMainTypeByUser(user: String) = {
    var types = Array("4conscn", "4conscom", "4conscomcn", "6numscom")
    var map = new scala.collection.mutable.HashMap[String, Array[(String, String, String, String, String, String)]]
    for (ty <- types) {
      var filePath = dataDir + "/" + ty + "_all.csv"
      map += (ty -> querySingleTypeByUser(filePath, user))
    }
    
    val result = new java.util.HashMap[String, OpenInterest]()
    map.foreach(
        dealer => {dealer._2.foreach(
            dd => {result.put(dealer._1 + dd._1, OpenInterest(dd._1,dd._2.toInt,dd._3,dd._4,dd._5,dd._6))
              }
            )
        }
    )
    result
  }

  def querySingleTypeByUser(dataPath: String, user: String) = {
    var splitter = "::"
    var minPartitions = 1
    var range = "1d"
    var data = sc.textFile(dataPath, minPartitions)

    var openinterest = data.map(line => line.split(splitter))
      .filter(columns => { columns.length > 7 && columns(7).equals(range) && columns(2).equals(user) })
      .map(fields => (fields(2), fields(3), fields(4), fields(5), fields(6), fields(7)))

    openinterest.take(1)
  }

  def applySimple(rankNumber: Int, user: String) = {

    var topDealers = queryAllTypeDealer(rankNumber)
    var userInfo = queryMainTypeByUser(user)

//    topDealers.filter(a => a._2.length > 0).foreach(a => println(a._1 + "," + a._2(0)))
//    userInfo.filter(a => a._2.length > 0).foreach(a => println(a._1 + "," + a._2(0)))
  }

  def queryAllTypeDealer(rankNumber: Int) = {
    var types = Array("4conscn", "4conscom", "4conscomcn", "6numscom")
    var map = new scala.collection.mutable.HashMap[String, Array[(String, String, String, String, String, String)]]
    for (ty <- types) {
      var filePath = dataDir + "/" + ty + "_all.csv"
      map += (ty -> querySingleTypeDealer(filePath, rankNumber))
    }

    //exclude null emails
    map = map.filter(dealer => "" != dealer._2(0)._1)
    
    val result = new java.util.HashMap[String, OpenInterest]()
    map.foreach(
        dealer => {dealer._2.foreach(
            dd => {result.put(dealer._1 + dd._1, OpenInterest(dd._1,dd._2.toInt,dd._3,dd._4,dd._5,dd._6))
              }
            )
        }
    )
    result
  }

  def querySingleTypeDealer(dataPath: String, rankNumber: Int) = {
    var splitter = "::"
    var minPartitions = 1
    var range = "1d"
    var data = sc.textFile(dataPath, minPartitions)

    var openinterests = data.map(line => line.split(splitter))
      .filter(columns => { columns.length > 7 && columns(7).equals(range) })
      .map(fields => (fields(2), fields(3), fields(4), fields(5), fields(6), fields(7)))

    openinterests.takeOrdered(rankNumber)(Ordering[Int].reverse.on(o => o._2.toInt))
  }
  
    case class OpenInterest(email:String, volume:Int, in:String, out:String, net_in:String, range:String)

    implicit val meegoWrites = new Writes[OpenInterest] {
      def writes(openinterest: OpenInterest) = Json.obj(
        "email" -> openinterest.email,
        "volume" -> openinterest.volume,
        "in" -> openinterest.in,
        "out" -> openinterest.out,
        "net_in" -> openinterest.net_in,
        "range" -> openinterest.range
        )
    }
  
    implicit val meegoReads: Reads[OpenInterest] =
      (
        (JsPath \ "email").read[String] and
        (JsPath \ "volume").read[Int] and
        (JsPath \ "in").read[String] and
        (JsPath \ "out").read[String] and
        (JsPath \ "net_in").read[String] and
        (JsPath \ "range").read[String])(OpenInterest.apply _)

}