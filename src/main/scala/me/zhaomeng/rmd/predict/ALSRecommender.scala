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
//    var dataPath = "/home/cuikexi/git_workspace/rmdsystem/data/ml-100k/u.data"
    var splitter = "\t"
    var minPartitions = 1

    var data = sc.textFile(dataPath, minPartitions)
    
//    var text = "zhaomeng, huangma, 10, 1000"
//    var data = sc.parallelize(text.split(","), 1)
    

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
    println(users)

    var unmark = carUserItem.union(useritem).reduceByKey((a, b) => a + b).filter(c => c._2 == 1).map(x => (x._1._1.toInt, x._1._2.toInt))

    val rank = 1
    val numIterations = 20
    val lambda = 0.01
    val model = ALS.train(rratings, rank, numIterations, lambda)

    val usersProducts = rratings.map {
      case Rating(user, product, rate) => (user, product)
    }

    val predictions = model.predict(unmark).map {
      case Rating(user, product, rate) => (user, product)
    }
  }

  def traingByALS(dataPath: String, modelPath: String) = {
    var splitter = "\t"
    var minPartitions = 1
    var data = sc.textFile(dataPath, minPartitions)

    var rratings = data.map(
      line => {
        line.split("\\s") match {
          case Array(user, item, rate, timestamp) => Rating(user.toInt, item.toInt, rate.toDouble)
        }
      })

    val rank = 1
    val numIterations = 20
    val lambda = 0.01
    val modle = ALS.train(rratings, rank, numIterations, lambda)

    modle.save(sc, modelPath)
  }

  def predictAllOthersByALS(dataPath: String, modelPath: String): Iterator[Rating] = {
    var splitter = "\t"
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
    var carUserItem = users.cartesian(items).map(x => ((x._1, x._2), 1))

    var useritem = data.map(line => line.split(splitter)).map(columns => ((columns(0), columns(1)), 1))

    var unmarkUserItem = carUserItem.union(useritem).reduceByKey((a, b) => a + b).filter(c => c._2 == 1).map(x => (x._1._1.toInt, x._1._2.toInt))

    val modle = MatrixFactorizationModel.load(sc, modelPath)

    //predict all
    val predictions = modle.predict(unmarkUserItem)

    predictions.toLocalIterator
  }
  
  def predictProductsForUserByALS(userId: Int, num: Int, modelPath: String):List[Rating] = {
      val modle = MatrixFactorizationModel.load(sc, modelPath)
  
      val predictions = modle.recommendProducts(userId, num)
  
      predictions.toList
   }
  
  def predictUsersForProductByALS(productId: Int, num: Int, modelPath: String):List[Rating] = {
      val modle = MatrixFactorizationModel.load(sc, modelPath)
  
      val predictions = modle.recommendUsers(productId, num)
  
      predictions.toList
   }
}