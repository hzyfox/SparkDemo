package Bayes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object PredictFromModel {
  def main(args: Array[String]): Unit = {
    BayesMetaUtil.printPredictHelper(args)
    val sparkSession = SparkSession.builder().appName("PredictFromModel").getOrCreate()
    val parentPath = args(0)
    val authPath = args(1)
    val childPaths = args.drop(2)
    val wordProbabilityPerKindRDDs = ListBuffer.empty[RDD[(String, Double)]]
    val authRDD = BayesMetaUtil.getAuthRDD(sparkSession.sparkContext, BayesMetaUtil.getName(parentPath, authPath))
    val kindProbabilityMap = BayesMetaUtil.getKindProbabilityMap(sparkSession.sparkContext, BayesMetaUtil.getKindProbabilityPath(parentPath))
    val wordCountPerKindMap = BayesMetaUtil.getWordCountsPerKindMap(sparkSession.sparkContext, BayesMetaUtil.getWordCountPerKindPath(parentPath))
    val (wordTotalCount, v) = BayesMetaUtil.getWordTotalCountAndV(sparkSession.sparkContext, BayesMetaUtil.getTotalCountAndVPath(parentPath))
    val hadoopFileSystem = FileSystem.get(new Configuration())
    for (childPath <- childPaths) {
      val inputPath = BayesMetaUtil.getInputPath(parentPath, childPath)
      val inputPathFormatByHadoop = new Path(inputPath)
      val wordProbabilityPerKindPath = BayesMetaUtil.getWordProbabilityPerKindPath(parentPath, childPath)
      if (!hadoopFileSystem.exists(inputPathFormatByHadoop)) {
        System.err.println(s"Warn: not exist path $inputPathFormatByHadoop")
      } else {
        wordProbabilityPerKindRDDs += BayesMetaUtil.getWordProbabilityPerKindRDD(sparkSession.sparkContext, wordProbabilityPerKindPath, BayesMetaUtil.getInputPath(parentPath, childPath))
      }
    }
    predict(sparkSession.sparkContext, hadoopFileSystem, authRDD, kindProbabilityMap, wordCountPerKindMap, v, wordTotalCount, wordProbabilityPerKindRDDs: _*)
  }

  def predict(sc: SparkContext,
              hadoopFileSystem: FileSystem,
              authRDD: RDD[(String, Int)],
              kindProbabilityMap: mutable.HashMap[String, Double],
              wordCountsPerKindMap: mutable.HashMap[String, Long],
              V: Long,
              wordTotalCount: Long,
              wordProbabilityPerKindRDDs: RDD[(String, Double)]*) = {

    val resultMap = new mutable.HashMap[String, Double]()
    val broadcastWordCountsPerKindMap = sc.broadcast(wordCountsPerKindMap)
    for (wordProbabilityPerKindRDD <- wordProbabilityPerKindRDDs) {
      authRDD.foreach(println)
      val proResult = sc.doubleAccumulator
      val finalResultRDD = authRDD.leftOuterJoin(wordProbabilityPerKindRDD)
      finalResultRDD.foreach(println)
      val name = wordProbabilityPerKindRDD.name
      finalResultRDD.foreach(x => {
        if (x._2._2.isDefined) {
          proResult.add(x._2._1 * Math.log(x._2._2.get))
        } else {
          //当文档中出现了没出现的词语时
          proResult.add(x._2._1 * Math.log(1d / (broadcastWordCountsPerKindMap.value(name) + V)))
        }
      })
      resultMap(name) = proResult.value
    }
    var maxP = ("", Double.MinValue)
    for (p <- resultMap) {
      if (p._2 > maxP._2) {
        maxP = p
      }
    }
    println(s"${authRDD.name} belong to " + maxP._1)
  }
}

//must invoke after BayesModel constructed




