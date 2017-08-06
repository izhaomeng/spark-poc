package me.zhaomeng.rmd.predict

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

class ALSRecommender(val url: String, val appName: String) {
  val sc: SparkContext = new SparkContext(url, appName)

  def this(url: String) {
    this(url, "ALSRecommender")
  }
  
  def applyALS( dataPath: String) = {
    var splitter = "::"
    var minPartitions = 1

    var data = sc.textFile(dataPath, minPartitions)

    var rratings = data.map(
      line => {
        line.split(splitter) match {
          case Array(user, item, rate, timestamp) => Rating(user.toInt, item.toInt, rate.toDouble)
        }
      })

    var users = data.map(line => line.split(splitter)).map(columns => columns(0)).distinct()
    var items = data.map(line => line.split(splitter)).map(columns => columns(1)).distinct()
    var useritem = data.map(line => line.split(splitter)).map(columns => ((columns(0), columns(1)), 1))

    var ratings = data.map(line => line.split(splitter)).map(columns => columns(2)).distinct

    var timstamps = data.map(line => line.split(splitter)).map(columns => columns(3)).distinct

    var carUserItem = users.cartesian(items).map(x => ((x._1, x._2), 1))

    var unmark = carUserItem.union(useritem).reduceByKey((a, b) => a + b).filter(c => c._2 == 1).map(x => (x._1._1.toInt, x._1._2.toInt))

    val rank = 1
    val numIterations = 20
    val lambda = 0.01
    val modle = ALS.train(rratings, rank, numIterations, lambda)

    val usersProducts = rratings.map {
      case Rating(user, product, rate) => (user, product)
    }

    val predictions = modle.predict(unmark).map {
      case Rating(user, product, rate) => (user, product)
    }

    predictions.foreach(println)
  }

  def traingByALS(dataPath: String, modelPath: String) = {
    var splitter = "::"
    var minPartitions = 1
    var data = sc.textFile(dataPath, minPartitions)

    var rratings = data.map(
      line => {
        line.split("\\s") match {
          case Array(user, item, rate, timestamp) => Rating(user.toInt, item.toInt, rate.toDouble)
        }
      })

    var users = data.map(line => line.split(splitter)).map(columns => columns(0)).distinct

    var items = data.map(line => line.split(splitter)).map(columns => columns(1)).distinct

    var useritem = data.map(line => line.split(splitter)).map(columns => ((columns(0), columns(1)), 1))

    var carUserItem = users.cartesian(items).map(x => ((x._1, x._2), 1))

    var unmark = carUserItem.union(useritem).reduceByKey((a, b) => a + b).filter(c => c._2 == 1).map(x => (x._1._1.toInt, x._1._2.toInt))

    val rank = 1
    val numIterations = 20
    val lambda = 0.01
    val modle = ALS.train(rratings, rank, numIterations, lambda)

    println(modle.productFeatures)
    println(modle.rank)
    println(modle.userFeatures)

    modle.save(sc, modelPath)
  }

  def predictByALS(dataPath: String, modelPath: String) = {
    var splitter = "::"
    var minPartitions = 1
    var data = sc.textFile(dataPath, minPartitions)

    var rratings = data.map(
      line => {
        line.split(splitter) match {
          case Array(user, item, rate, timestamp) => Rating(user.toInt, item.toInt, rate.toDouble)
        }
      })

    var users = data.map(line => line.split(splitter)).map(columns => columns(0)).distinct

    var items = data.map(line => line.split(splitter)).map(columns => columns(1)).distinct

    var useritem = data.map(line => line.split(splitter)).map(columns => ((columns(0), columns(1)), 1))

    var carUserItem = users.cartesian(items).map(x => ((x._1, x._2), 1))

    var unmark = carUserItem.union(useritem).reduceByKey((a, b) => a + b).filter(c => c._2 == 1).map(x => (x._1._1.toInt, x._1._2.toInt))

    val modle = MatrixFactorizationModel.load(sc, modelPath)

    val predictions = modle.predict(unmark).map {
      case Rating(user, product, rate) => (user, product)
    }

    predictions.foreach(println)
  }
}