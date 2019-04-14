package generator

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * create with generator
  * USER: husterfox
  */
object Accuracy {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("please input <actual_file> <predict_file>")
      System.exit(1)
    }
    val actualFile = args(0)
    val predictFile = args(1)
    val actualList = new ListBuffer[(String, Int)]
    Source.fromFile(actualFile).getLines().foreach(line => {
      if (line.split("""\^\^""").length == 3) {
        val strs = line.split("""\^\^""")
        actualList += ((strs(0), if (Integer.valueOf(strs(1)) > 2) 1 else 0))
      }
    })
    val orderedAcutalList = actualList.sortBy(_._1)(Ordering.String)
    val predictList = new ListBuffer[(String, Int)]
    Source.fromFile(predictFile).getLines().foreach(line => {
      val strs = line.split(" ")
      val build = new StringBuilder
      if (strs.length > 2) {
        for (i <- 0 until strs.length - 1) {
          if (i < strs.length - 2) {
            build.append(strs(i))
            build.append(" ")
          } else {
            build.append(strs(i))
          }
        }
      } else {
        build.append(strs(0))
      }
      var value = 0
      try {
        value = Integer.valueOf(strs(strs.length - 1))
      } catch {
        case e: Exception =>
          println(line)
      }
      predictList += ((build.toString(), value))
    })
    val orderedPredictList = predictList.sortBy(_._1)(Ordering.String)
    if (orderedAcutalList.size != orderedPredictList.size) {
      print(s"size is not equal predict is ${orderedPredictList.size} acutal is ${orderedAcutalList.size} ")
      System.exit(1)
    }
    var cnt = 1
    val orderedPredictListIter = orderedPredictList.iterator
    val orderedAcutalListIter = orderedAcutalList.iterator
    var zero = 0
    var one = 1
    while (orderedAcutalListIter.hasNext && orderedPredictListIter.hasNext) {
      val acutal = orderedAcutalListIter.next()
      val predict = orderedPredictListIter.next()
      if (acutal._1 != predict._1) {
        println(s"acutal: $acutal predict: $predict")
      }
      if (acutal._2 == predict._2) {
        if (acutal._2 == 1) {
          one += 1
        } else {
          zero += 1
        }
        cnt = cnt + 1
      }
    }
    println(s"accuracy is  ${cnt.asInstanceOf[Double] / orderedPredictList.size} one: $one  ratio: ${one / cnt.asInstanceOf[Double]}   zero: $zero ratio: ${zero / cnt.asInstanceOf[Double]}")
  }

}
