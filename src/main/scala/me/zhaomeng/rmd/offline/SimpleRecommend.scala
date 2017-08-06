package me.zhaomeng.rmd.offline

import me.zhaomeng.rmd.simple.SimpleRecommender

object SimpleRecommend {

  //    case class OpenInterest(email:String, volume:Int, in:String, out:String, net_in:String, range:String)
  //    case class OpenInterest(email:String, volume:String, in:String, inList:String, out:String, outList:String, net_in:String, range:String) 

  def main(args: Array[String]) {
    runSimple()
  }

  def runSimple() {
    var url  = "local[1]"
    var name = "recommend"
    var recomalg = new SimpleRecommender(url, name)
    var dataDir = "data"
    var num = 10

    var user = "kf@midai.com"

    recomalg.applySimple(num, user)
  }

}