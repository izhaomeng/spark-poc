package me.zhaomeng.rmd.offline

import me.zhaomeng.rmd.predict.ALSRecommender

object Recommend {
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      runInLocal()
    } else {
      runInCluter(args)
    }
  }

  def runInLocal() {
    var url  = "local[1]"
    var als = new ALSRecommender(url)
    
    var user = "kf@midai.com"

    var filePath = "data/ml-100k/u.data"
    var modelPath = "model/als.model"
    
    als.applyALS(filePath)

    //    recomalg.traingByALS(filePath, modelPath)
    //    recomalg.predictByALS(filePath, modelPath)

  }

  def runInCluter(args: Array[String]) {
    var url  = "spark://127.0.0.1:7077"
    var name = "als recommend"
    
    var als = new ALSRecommender(url, name)
    
    var user = "kf@midai.com"

    var filePath =  "/home/cuikexi/scala_workspace/recommendation/data/ml-100k/u.data"
    var modelPath = "model/als.model"
    
    als.applyALS(filePath)

    //    recomalg.traingByALS(filePath, modelPath)
    //    recomalg.predictByALS(filePath, modelPath)
  }
}