package rmdsystem.study

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import scala.collection.immutable.Queue


object Fresh {
  def main(args: Array[String]){
    var range = (1 to 5)
    var list = List(1,2,3,4,5)
    var set = Set(1,2,3,4,5)
    var map = Map(1->2,2->3,3->4,4->5,5->6)
    
    var data1 = (3 to 10)
    var data2 = (4 to 11)

    println(range)
    println(list)
    println(set)
    println(map)
    
    var data4 = data1.union(data2).foreach(println)
    
  }
}