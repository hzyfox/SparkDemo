package Bayes


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * create with Bayes
  * USER: husterfox
  */
//Used on the driver
object BayesMetaUtil {
  //名称不得以下划线开头
  val wordProbabilityPerKindSuffix = "word_probability_per_kind"
  val wordCountsPerKindSuffix = "word_count_per_kind"
  val kindProbabilitySuffix = "kind_probability"
  val wordTotalCountAndVSuffix = "word_total_count_V"
  val parentPathNameForMeta = "meta"

  def getWordCountsPerKindMap(sc: SparkContext, path: String): mutable.HashMap[String, Long] = {
    val wordCountPerKindMap = mutable.HashMap.empty[String, Long]
    sc.textFile(path).collect().foreach(str => {
      val strs = str.split(" ")
      wordCountPerKindMap(strs(0)) = strs(1).toLong
    })
    wordCountPerKindMap
  }

  def getKindProbabilityMap(sc: SparkContext, path: String): mutable.HashMap[String, Double] = {
    val kindProbabilityMap = mutable.HashMap.empty[String, Double]
    sc.textFile(formatPathByHadoop(path)).collect().foreach(x => {
      val strs = x.split(" ")
      kindProbabilityMap(strs(0)) = strs(1).toDouble
    })
    kindProbabilityMap
  }

  def saveMapToSink[A, B](map: mutable.Map[A, B], path: String, sc: SparkContext, hadoopFileSystem: FileSystem): Unit = {
    checkPathExist(hadoopFileSystem, path)
    sc.parallelize(map.toSeq).map(kv => kv._1 + " " + kv._2).saveAsTextFile(path)
    println(s"save map to $path")
  }

  def saveWordTotalCountAndV(wordTotalCount: Long, V: Long, path: String, sc: SparkContext, hadoopFileSystem: FileSystem) = {
    checkPathExist(hadoopFileSystem, path)
    val wordTotalCountAndV = ListBuffer(wordTotalCount, V)
    sc.parallelize(wordTotalCountAndV).map(_.toString).saveAsTextFile(path)
  }

  def getWordTotalCountAndV(sc: SparkContext, path: String): (Long, Long) = {
    val dataArray = sc.textFile(path).take(2)
    (dataArray(0).toLong, dataArray(1).toLong)
  }

  def checkPathExist(hadoopFileSystem: FileSystem, path: String): Unit = {
    val targetPath = new Path(path)
    if (hadoopFileSystem.exists(targetPath)) {
      hadoopFileSystem.delete(targetPath, true)
    }
  }

  def getWordProbabilityPerKindRDD(sc: SparkContext, path: String, name: String): RDD[(String, Double)] = {
    sc.textFile(path).map(kv => {
      val strs = kv.split(" ")
      (strs(0), strs(1).toDouble)
    }).setName(name)
  }

  def getAuthRDD(sc: SparkContext, path: String): RDD[(String, Int)] = {
    sc.textFile(path).filter(_!="").flatMap(_.split("\\s")).map((_, 1)).reduceByKey(_ + _).setName(formatPathByHadoop(path))
  }

  def getWordCountPerKindPath(parent: String): String = {
    new Path(parent, wordCountsPerKindSuffix).toString
  }

  def getKindProbabilityPath(parent: String): String = {
    new Path(parent, kindProbabilitySuffix).toString
  }

  def getTotalCountAndVPath(parent: String): String = {
    new Path(parent, wordTotalCountAndVSuffix).toString
  }

  def getWordProbabilityPerKindPath(parent: String, child: String) = {
    new Path(new Path(parent, parentPathNameForMeta + child), wordProbabilityPerKindSuffix).toString
  }

  def getInputPath(parent: String, child: String): String = {
    new Path(parent, child).toString
  }

  def getName(parent: String, child: String): String = {
    getInputPath(parent, child)
  }

  def formatPathByHadoop(path: String): String = {
    new Path(path).toString
  }
  def printTrainHelper(args: Array[String]): Unit ={
    if (args.length < 3) {
      System.err.println("该训练器需要一个父目录、两个训练文件子目录 请指定父目录 两个训练子目录")
      System.err.println("please input hdfs path: <parentPath>  <data0Path> <data1Path>")
      System.err.println("路径请以/结尾 开头不要带/")
      System.err.println("eg. file:/home/admin/data/  china/ japan/")
      System.err.println("eg. hdfs:/home/admin/data/  china/ japan/")
      System.exit(-1)
    }
  }
  def printPredictHelper(args: Array[String]): Unit ={
    if (args.length < 4) {
      System.err.println("该预测器需要一个父目录、测试文件子目录和两个训练文件子目录 请指定父目录 测试文件子目录 以及两个训练文件子目录")
      System.err.println("please input hdfs path: <parentPath> <authPath> <data0Path> <data1Path>")
      System.err.println("路径请以/结尾 开头不要带/")
      System.err.println("eg. file:/home/admin/data/ auth/ china/ japan/")
      System.err.println("eg. hdfs:/home/admin/data/ auth/ china/ japan/")
      System.exit(-1)
    }
  }
  def printGeneratorHelper(args: Array[String]): Unit ={
    if (args.length < 4) {
      System.err.println("该生成器生成两个训练文件到训练子目录，生成一个测试文件到测试子目录")
      System.err.println("该生成器器需要一个父目录、测试文件子目录和两个训练文件子目录 请指定父目录 测试文件子目录 以及两个训练文件子目录")
      System.err.println("please input hdfs path: <parentPath> <authPath> <data0Path> <data1Path>")
      System.err.println("路径请以/结尾 开头不要带/")
      System.err.println("eg. file:/home/admin/data/ auth/ china/ japan/")
      System.err.println("eg. hdfs:/home/admin/data/ auth/ china/ japan/")
      System.exit(-1)
    }
  }
}
