package rmdsystem.study

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType

object SparkDB {
  def main(args: Array[String]) {
    //    json("data/hisasset.json")
    //    csv("data/hisasset.csv")
    //    text("data/hisasset.txt")

            mysql()
//    csv2()
//    json2()

  }

  def mysql() {
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)

    var url = "jdbc:mysql://192.168.1.53/OpenInterestRecord"
    var table = "4conscn_all"
    var connectionProps = new Properties
    connectionProps.put("driver", "com.mysql.jdbc.Driver")
    connectionProps.put("url", url)
    connectionProps.put("dbtable", table)
    connectionProps.put("user", "awei")
    connectionProps.put("password", "awei")

    var df = sqlContext.read.jdbc(url, table, connectionProps).toDF()
    
    sqlContext.sql("select * from 4conscn_all where range = '1d' order by volume desc limit 10").foreach { a => println(a) }
  }

  def json(jsonFile: String) {
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)

    var df = sqlContext.read.json(jsonFile).toDF();

    print(df.first())
  }

  def csv(csvFile: String) {
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)

    var df = sqlContext.read.csv(csvFile).toDF();

    print(df.first())
  }

  def text(textFile: String) {
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)

    var df = sqlContext.read.text(textFile).toDF();

    print(df.first())
  }
  
  def csv2() = {
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)

    var csvFile = "data/6com.csv"

    var df = sqlContext.read.option("header", "true").option("delimiter", "|").csv(csvFile);
    df = df.withColumn("VolumeT", df("volume").cast(IntegerType)).drop("volume").withColumnRenamed("VolumeT", "volume").toDF()
    df.createOrReplaceTempView("openinterest")
    df.printSchema()
    sqlContext.sql("select * from openinterest where range = '1d' order by volume desc limit 10").foreach { a => println(a) }

  }

  def json2() = {
    var sc = new SparkContext("local[1]", "spdb")
    var sqlContext = new SQLContext(sc)

    var jsonFile = "data/6numscom.json"
    
    var df = sqlContext.read.json(jsonFile);
    df = df.withColumn("VolumeT", df("volume").cast(IntegerType)).drop("volume").withColumnRenamed("VolumeT", "volume").toDF()
    df.createOrReplaceTempView("openinterest")
    df.printSchema()
    sqlContext.sql("select * from openinterest where range = '1d' order by volume desc limit 10").foreach { a => println(a) }

  }

}