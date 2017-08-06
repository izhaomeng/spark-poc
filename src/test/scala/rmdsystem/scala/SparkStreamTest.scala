package rmdsystem.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.junit.Test
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.Queue
import me.zhaomeng.rmd.predict.ALSRecommender
import java.util.Date
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.ChronoUnit

class SparkStreamTest {
  
  @Test
  def testRdd() = {
      listRdd
  }
    
  @Test
  def testDf() = {
      listDf
  }
  
  @Test
  def testJson() = {
    json
  }
  
  @Test
  def testCsv() = {
    csv
  }
  
    @Test
  def testStream() = {
    textStream
  }
    
  @Test
  def mlRecommend() = {
    var url  = "local[1]"
    var als = new ALSRecommender(url)
    
    var userId = 3
    var productId = 4
    var suggestionNumber = 3
    
    var dataPath = "/home/cuikexi/git_workspace/rmdsystem/data/ml-100k/u.data"
    var modelPath = "/home/cuikexi/git_workspace/rmdsystem/model/als.model"
    
    var d1 = LocalTime.now()
    als.traingByALS(dataPath, modelPath)
    
    var d2 =  LocalTime.now()
    als.predictAllOthersByALS(dataPath, modelPath)
    
    var d3 =  LocalTime.now()
    als.predictProductsForUserByALS(userId, suggestionNumber, modelPath)
    
    var d4 =  LocalTime.now()
    als.predictUsersForProductByALS(productId, suggestionNumber, modelPath)
    
    var d5 =  LocalTime.now()
    println("train time:" + d1.until(d2, ChronoUnit.SECONDS) + "s")
    println("predict time:" + d2.until(d3, ChronoUnit.SECONDS) + "s")
    println("suggest products time:" + d3.until(d4, ChronoUnit.MILLIS) + "ms")
    println("suggest users time:" + d4.until(d5, ChronoUnit.MILLIS) + "ms")
  }
   
  val  delimiter: String = ","
  val  csvEscape: Boolean = true
  val  compress: Boolean = true
  val  header: Option[String] = None
  val  maxColumnLength: Option[Int] = None
  
  def listRdd(){
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)
  
    var listStr1 = """zm,zn,zq""" 
    var list = listStr1.split(",").toList
    var rdd = sc.parallelize(list, 2)
    var max = rdd.max()
    println(max)
  }

  def listDf(){
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)
  
    var listStr1 = """zm,male,12;zn,male,5;zp,male,20""" 
    var list = listStr1.split(";").toList.map(line => line.split(",")).collect{
      case s:Array[String] => org.apache.spark.sql.Row(s(0),s(1),s(2).toInt)
    }
    
    var rowRDD = sc.parallelize(list, 2)
    val schema = StructType(Array(StructField("name",StringType,true)
         ,StructField("sex",StringType,true),
         StructField("year",IntegerType,true)))
    
    var df = sqlContext.createDataFrame(rowRDD, schema)

    df.createOrReplaceTempView("op")
    df.printSchema()
    var rdf = sqlContext.sql("select * from op where name != 'zl' order by year desc limit 10")
    rdf.foreach { a => println(a) }
  }
  
  def csv() = {
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)
  
    var csvFile = "data/6com.csv"
    var df = sqlContext.read.option("header", "true").option("delimiter", "|").csv(csvFile)
    
    df = df.withColumn("VolumeT", df("volume").cast(IntegerType)).drop("volume").withColumnRenamed("VolumeT", "volume").toDF()
    df.createOrReplaceTempView("op")
    df.printSchema()
    var rdf = sqlContext.sql("select * from op where range = '1d' order by volume desc limit 10")
    rdf.foreach(a => println(a))
   }
  
  def json() = {
   var sc = new SparkContext("local[1]", "spdb")
   var sqlContext = new SQLContext(sc)
  
   var jsonFile = "data/6numscom.json"
   var df = sqlContext.read.json(jsonFile)
   
   df = df.withColumn("VolumeT", df("volume").cast(IntegerType)).drop("volume").withColumnRenamed("VolumeT", "volume").toDF()
   df.createOrReplaceTempView("op")
   df.printSchema()
   sqlContext.sql("select * from op where range = '1d' order by volume desc limit 10").foreach { a => println(a) }
   }
  
  def textStream() = {
   var scf = new SparkConf().setAppName("streamT").setMaster("local[1]")
   var streamContext = new StreamingContext(scf, Seconds(1))
   
   var listStr1 = """zm,zn,zq""" 
   var list = listStr1.split(",").toList
   var rdd = streamContext.sparkContext.parallelize(list, 2)
   var rdd2 = streamContext.sparkContext.parallelize(list, 2)
   var queue = Queue(rdd,rdd2)
//   var linesStream:DStream[String]= streamContext.queueStream(queue, true)
//   var linesStream:DStream[String] = streamContext.textFileStream("file:///home/cuikexi/git_workspace/rmdsystem/data")
   var linesStream = streamContext.socketTextStream("localhost", 10001)
   
   var wordsStream = linesStream.flatMap { line => line.split(" ") }   
   var wStream = wordsStream.map(x => (x, 1))
   var wc = wStream.reduceByKey((x, y) => x + y)
   wc.print()
   streamContext.start()
   streamContext.awaitTermination()
  }
  
}