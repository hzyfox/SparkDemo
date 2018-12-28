package Bayes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * create with Bayes
  * USER: husterfox
  */

//在多项式模型中， 设某文档d=(t1,t2,…,tk)，tk是该文档中出现过的单词，允许重复，则
//
//先验概率P(c)= 类c下单词总数/整个训练样本的单词总数
//
//类条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/(类c下单词总数+|V|)
//
//V是训练样本的单词表（即抽取单词，单词出现多次，只算一个），|V|则表示训练样本包含多少种单词。在这里，m=|V|, p=1/|V|。
//
//P(tk|c)可以看作是单词tk在证明d属于类c上提供了多大的证据，而P(c)则可以认为是类别c在整体上占多大比例(有多大可能性)。
object BayesBasedOnWord {
  def main(args: Array[String]): Unit = {
    BayesMetaUtil.printTrainHelper(args)
    val sparkSession = SparkSession.builder().appName("BayesBasedOnWord").getOrCreate()
    val hadoopConf = new Configuration()
    val hadoopFileSystem = FileSystem.get(hadoopConf)
    //用于记录每种类文件的单词数
    val wordCountsPerKindMap = new mutable.HashMap[String, Long]()
    //用于记录每种类文件出现的概率
    val kindProbabilityMap = new mutable.HashMap[String, Double].empty
    //用于存放训练集总单词数
    var wordTotalCount: Long = 0L
    //用于记录存放训练集总单词种类数，即不重复的单词数
    var V: Long = 0L
    //第0个参数是父目录，后面的是训练目录名
    val parentPath = args(0)
    val childPaths = args.drop(1)
    //计算每类文件的单词数,记录到wordCountsPerKindMap中，并计算所有文档单词数(包含重复)记录到WordTotalCount
    for (childPath <- childPaths) {
      val inputPath = BayesMetaUtil.getInputPath(parentPath, childPath)
      val inputPathFormatByHadoop = new Path(inputPath)
      if (!hadoopFileSystem.exists(inputPathFormatByHadoop)) {
        println(s"Warn: not exist path $inputPathFormatByHadoop")
      } else {
        val count = sparkSession.sparkContext.textFile(inputPathFormatByHadoop.toString).flatMap(_.split(" ")).count()
        wordCountsPerKindMap(inputPathFormatByHadoop.toString) = count
      }
    }
    for (wordCount <- wordCountsPerKindMap) {
      println(wordCount._1 + " word number is : " + wordCount._2)
      wordTotalCount += wordCount._2
    }
    //将wordCountsPerKindMap存到底层存储
    BayesMetaUtil.saveMapToSink(wordCountsPerKindMap,
      BayesMetaUtil.getWordCountPerKindPath(parentPath),
      sparkSession.sparkContext, hadoopFileSystem)
    println(s"所有文档总词数为: $wordTotalCount")
    //计算每个种类的概率
    for (wordCount <- wordCountsPerKindMap) {
      kindProbabilityMap(wordCount._1) = wordCount._2.asInstanceOf[Double] / wordTotalCount
      println(wordCount._1 + " Probability is : " + kindProbabilityMap(wordCount._1))
    }

    //将kindProbabilityMap存到底层存储
    BayesMetaUtil.saveMapToSink(kindProbabilityMap,
      BayesMetaUtil.getKindProbabilityPath(parentPath),
      sparkSession.sparkContext, hadoopFileSystem)

    //计算所有文档的单词数(不重复)，记录到V
    var preRdd: RDD[String] = null
    for (childPath <- childPaths) {
      val inputPathFormatByHadoop = new Path(BayesMetaUtil.getInputPath(parentPath, childPath))
      if (!hadoopFileSystem.exists(inputPathFormatByHadoop)) {
       System.err.println(s"Warn: not exist path $inputPathFormatByHadoop")
      } else {
        if (preRdd != null) {
          preRdd = sparkSession.sparkContext.textFile(inputPathFormatByHadoop.toString).flatMap(_.split(" ")).distinct().union(preRdd).distinct()
        } else {
          preRdd = sparkSession.sparkContext.textFile(inputPathFormatByHadoop.toString).flatMap(_.split(" ")).distinct()
        }
      }
    }
    if (preRdd != null) {
      V = preRdd.count()
      println(s"所有文档的总词数(不重复): $V")
    }

    //wordTotalCount 和 V都计算出来了，存入到底层存储
    BayesMetaUtil.saveWordTotalCountAndV(wordTotalCount, V,
      BayesMetaUtil.getTotalCountAndVPath(parentPath),
      sparkSession.sparkContext, hadoopFileSystem)


    //计算每个单词的数目，及其概率(word,p),保存到文档中
    for (childPath <- childPaths) {
      val inputPath = BayesMetaUtil.getInputPath(parentPath, childPath)
      val inputFilePathFormatByHadoop = new Path(inputPath)
      val formatFilePath = inputFilePathFormatByHadoop.toString
      if (!hadoopFileSystem.exists(inputFilePathFormatByHadoop)) {
        System.err.println(s"Warn: not exist path $inputFilePathFormatByHadoop")
      } else {
        val wordProbabilityPerKindFilePath = BayesMetaUtil.getWordProbabilityPerKindPath(parentPath,childPath)
        //outputPath.toString会把多余'/'去掉 导致wordCounts访问出现key not found异常，所以使用outPutStr
        BayesMetaUtil.checkPathExist(hadoopFileSystem, wordProbabilityPerKindFilePath)
        sparkSession.sparkContext.textFile(inputFilePathFormatByHadoop.toString).
          flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)
          .mapValues(count => (count + 1).asInstanceOf[Double] / (wordCountsPerKindMap(formatFilePath) + V))
          .map(x => x._1 + " " + x._2)
          .saveAsTextFile(wordProbabilityPerKindFilePath)
      }
    }
  }


}
