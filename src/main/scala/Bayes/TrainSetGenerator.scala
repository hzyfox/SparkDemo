package Bayes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


object TrainSetGenerator {
  def main(args: Array[String]): Unit = {
    BayesMetaUtil.printGeneratorHelper(args)
    val parentFilePath = args(0)
    val authFilePath = new Path(BayesMetaUtil.getInputPath(parentFilePath, args(1)))
    val chinaFilePath = new Path(BayesMetaUtil.getInputPath(parentFilePath, args(2)))
    val japanFilePath = new Path(BayesMetaUtil.getInputPath(parentFilePath, args(3)))

    val hadoopConf = new Configuration()
    val hadoopFileSystem = FileSystem.get(hadoopConf)
    BayesMetaUtil.checkPathExist(hadoopFileSystem, authFilePath.toString)
    BayesMetaUtil.checkPathExist(hadoopFileSystem, chinaFilePath.toString)
    BayesMetaUtil.checkPathExist(hadoopFileSystem, japanFilePath.toString)
    val sparkSession = SparkSession.builder().appName("BayesTrainSetGenerator").master("local").getOrCreate()
    val authFile = sparkSession.sparkContext.parallelize(Seq("HeBei"
      , "HeBei", "HeBei", "Japan", "Japan", "Japan"))
    val chinaFile = sparkSession.sparkContext.parallelize(Seq("China"
      , "Jiangxi", "Hangzhou", "Shanghai", "Hebei", "Wuhan", "china"))
    val japanFile = sparkSession.sparkContext.parallelize(Seq("Japan"
      , "Tokyo", "Hokkaido", "Nara", "Nara"))
    authFile.saveAsTextFile(authFilePath.toString)
    chinaFile.saveAsTextFile(chinaFilePath.toString)
    japanFile.saveAsTextFile(japanFilePath.toString)
    println(s"auth file in $authFilePath|Japan file in $japanFilePath|china file in $chinaFilePath")
    hadoopFileSystem.close()
  }
}
