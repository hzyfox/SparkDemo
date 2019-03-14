package Bayes


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, TextInputFormat}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.runtime.{universe => ru}

/**
  * create with Bayes
  * USER: husterfox
  */
object Test {
  def main(args: Array[String]): Unit = {
    //    val demoFile = new FileWriter("/Users/husterfox/Downloads/demoFile.txt", true)
    //    for (i <- 0 until 10000000) {
    //      demoFile.write(s"$i I am a String!!!! hahahahahahahahahahahahahahahahahahahahahahhahahahah\n")
    //    }
    //    demoFile.close()
    val conf = new SparkConf().setMaster("local").setAppName("11")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/Users/husterfox/Downloads/20190125",100)
    rdd.count()
    println(rdd.partitions.length)
  }
  def spark() = {
    val sc = new SparkContext()
    sc.textFile("/Users/husterfox/Downloads/demoFile.txt")
  }
  def test(): Unit = {

    val path = new Path("/Users/husterfox/Downloads/demoFile.txt")
    val uri = path.toUri
    val conf = new Configuration()
    val fileSystem = FileSystem.get(uri, conf)
    val fileStatus = fileSystem.listStatus(path)(0)
    println(s"filename is $fileStatus")
    val totalSize = fileStatus.getLen
    val blockSize = fileStatus.getBlockSize
    val inputFormat = ReflectionUtils.newInstance(classOf[TextInputFormat], conf)
      .asInstanceOf[InputFormat[LongWritable, Text]]
    val makeSpilt = inputFormat.getClass.getSuperclass.getDeclaredMethod("makeSplit",classOf[Path],classOf[Long],classOf[Long],classOf[Array[String]],classOf[Array[String]])
    makeSpilt.setAccessible(true)
    makeSpilt.invoke(inputFormat)
    println(makeSpilt)
    println(inputFormat.getClass)
    println(s"blocksize is $blockSize")
    //val firstSplit =
  }
  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
}

