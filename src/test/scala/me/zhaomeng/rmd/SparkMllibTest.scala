package me.zhaomeng.rmd

import org.junit.Test
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.LDA
import me.zhaomeng.spark.TextAnalyser
import scala.collection.JavaConverters
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.wltea.analyzer.lucene.IKAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.regression.LassoWithSGD
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.optimization.SquaredL2Updater

class SparkMllibTest {
  
  def toWordVectors(datas: Array[String]) = {
    var analyser = new TextAnalyser

    var wcs = datas.map(data => {
      var tc = analyser.analyzeWithCount(data)
      var tcm = JavaConverters.mapAsScalaMapConverter(tc).asScala
      tcm
    })

    var sets = wcs.map(wc => wc.keySet).flatMap { s => s }.toSet.toArray

    var wv = wcs.map { x => sets.map { word => if (x.contains(word)) (word -> x.get(word).get) else (word -> 0D) } }

    var words = wv.take(1).map(wl => wl.map(w => w._1))
    var vectors = wv.map(wl => wl.map(w => w._2))

    (words(0), vectors)
  }

  def toWordVector(document: String, words: Array[String]) = {
    var analyser = new TextAnalyser

    var tc = analyser.analyzeWithCount(document)
    var tcm = JavaConverters.mapAsScalaMapConverter(tc).asScala

    var wv = words.map { word => if (tcm.contains(word)) (word -> tcm.get(word).get) else (word -> 0D) } 

    var vector = wv.map(w => w._2)

    vector
  }
  
    @Test
  def testWord2Vec = {
    var sc = new SparkContext("local[1]", "word2vec")
    var documents = List("机智机器学习（Tencent Machine Learning）是基于超大规模计算资源和性能领先的并行计算平台，结合大量最流行的传统算法与深度学习算法，一站式简化用户对算法的接口调用、可视化、参数调优等自动化任务管理的开放平台．",
                  "DI-X（Data Intelligence X）是基于腾讯云强大计算能力的一站式深度学习平台。它通过可视化的拖拽布局，组合各种数据源、组件、算法、模型和评估模块，让算法工程师和数据科学家在其之上，方便地进行模型训练、评估及预测。目前支持 TensorFlow、Caffe、Torch 三大深度学习框架，并提供相应的常用深度学习算法和模型。DI-X 可以帮助您快速接入人工智能的快车道，释放数据潜力。",
                  "腾讯机器翻译（Tencent Machine Translation）结合了神经机器翻译和统计机器翻译的优点，从大规模双语语料库自动学习翻译知识，实现从源语言文本到目标语言文本的自动翻译，目前可支持中英双语互译。")
    
    var analyzedDocuments = documents.map {   
      document => 
          var analyzer = new IKAnalyzer
          var tokens = analyzer.tokenStream("text", document)
          var analyzedDocument = List[String]()
          tokens.reset()
          while(tokens.incrementToken()){
               var t = tokens.getAttribute(classOf[CharTermAttribute])
               analyzedDocument = analyzedDocument.::(t.toString())
          } 
          analyzedDocument
      }
    analyzedDocuments.foreach(println)
    
    var data = sc.parallelize(analyzedDocuments, 2)
    var word2vec = new Word2Vec
    word2vec.setVectorSize(50)
    var model = word2vec.fit(data)
    model.getVectors.foreach(((a)) => {
      print(a._1 + ":(" + a._2.length + ")")
      a._2.foreach { f => print(f+" ") }
      println 
      }
    )
    
    
//    var modelPath = "model/word2vec.model"
//    model.save(sc, modelPath)
    
//    model = Word2VecModel.load(sc, modelPath)
//    model.findSynonyms("算法", 3).foreach(println) 
  }

  @Test
  def testAna() {
    var sc = new SparkContext("local[1]", "lda")

    var datas = Array(
      "hello zhaomeng zhao zhao",
      "hello zhaomeng zhao",
      "huiquba, nihao",
      "nihao, zenmele",
      "zhao zhao, hello",
      "zhaomeng, hello",
      "nihao wuda")

    
    val wvectors = toWordVectors(datas)
    val wvector = wvectors._2
    var words = wvectors._1

    wvector.foreach(x => { x.foreach { y => print(y) 
                                       print(' ')}
                           println
                           })

    val data = sc.parallelize(wvector, 2)
    val parsedData = data.map(wv => Vectors.dense(wv.map(_.toString().toDouble)))
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()
    
    val topicNumbers = 3
    val ldaModel = new LDA().
      setK(topicNumbers).
      setDocConcentration(5).
      setTopicConcentration(5).
      setMaxIterations(20).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").run(corpus)

    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    val vocabSize = ldaModel.vocabSize
    for (topic <- Range(0, topicNumbers)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, vocabSize)) {
        print(" " + topics(word, topic));
      }
      println()
    }

    val modelPath = "model/lda.model"
    ldaModel.save(sc, modelPath)

    val ldamodel = DistributedLDAModel.load(sc, modelPath)

    var document = "hello, zhao zhao"
    var wv = toWordVector(document, words)
    wv.foreach(x => {
        print(x)
        print(' ')
    })
    
    var v = ldamodel.toLocal.topicDistribution(Vectors.dense(wv.map(_.toString).map(_.toDouble)))
    
    println(v)
  }

  @Test
  def testLDA() {
    var sc = new SparkContext("local[1]", "lda")

    val data = sc.textFile("data/text.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    val ldaModel = new LDA().
      setK(3).
      setDocConcentration(5).
      setTopicConcentration(5).
      setMaxIterations(20).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").
      run(corpus)

    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    val vocabSize = ldaModel.vocabSize
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, vocabSize)) {
        print(" " + topics(word, topic));
      }
      println()
    }

  }
  
  @Test
  def testLogisticRegression = {
    var sc = new SparkContext("local[1]", "logisticregression")
    var labelRows = List("1, 10, 8, 3, 1, 0",
        "0, 12, 8, 3, 1, 1",
        "0, 10, 0, 3, 1, 0",
        "1, 2, 1, 3, 5, 0",
        "0, 7, 8, 7, 1, 0",
        "1, 7, 8, 4, 2, 1",
        "1, 0, 8, 3, 4, 0"
      )
    
    var points = labelRows.map {   
      row => 
        var s = row.split(",")
        LabeledPoint(s.take(1).apply(0).toDouble, Vectors.dense(s.takeRight(5).map(_.toDouble)))
      }
    points.foreach(println)
    
    var data = sc.parallelize(points, 2)
    var lr = new LogisticRegressionWithSGD
    var model =  lr.run(data)
    
    var testData = Vectors.dense( 1, 2, 3, 4, 0)
    var predictResult = model.predict(testData)
    println(predictResult)
  }
  
  
 case class DT(x1:Double, x2:Double, y:Double)
 
  @Test
  def testLinearRegression  {
    var sc = new SparkContext("local[1]", "linearregression")
    var labelRows = List("1, 1,  2",
        "2, 2,  3",
        "3, 3,  4",
        "4, 4,  5",
        "5, 5,  7",
        "6, 6,  7",
        "7, 7,  8",
        "8, 8,  9",
        "9, 9,  10",
        "10, 10,  11",
        "12, 12,  13",
        "13, 13,  14",
        "14, 14,  15",
        "15, 15,  16",
        "16, 16,  17"
      )
    
    var points = labelRows.map {   
      row => 
        var s = row.split(",")
        LabeledPoint(s.takeRight(1).apply(0).toDouble, Vectors.dense(s.take(2).map(_.toDouble)))
      }
    points.foreach(point => println((point.features, point.label)))
    
    var data = sc.parallelize(points, 1)
    var lr = new LinearRegressionWithSGD
    lr.optimizer.setNumIterations(200).setUpdater(new SquaredL2Updater)
    var model =  lr.run(data)
    println(model.toString())
    
    var predictResult = points.map { 
      point => 
          var label = point.label
          var feature = point.features
          var predict = model.predict(feature)
          new DT(feature(0), feature(0), predict)
    }
    
    var result = sc.parallelize(predictResult)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    var df = sqlContext.createDataFrame(predictResult)
    df.createOrReplaceTempView("tt")
    var sqlr = sqlContext.sql("select x1,x2,y from tt")
    sqlr.foreach { x => println(x) }
    
    predictResult.foreach(println)
//    println(model.toPMML())
  }
  
  
}
