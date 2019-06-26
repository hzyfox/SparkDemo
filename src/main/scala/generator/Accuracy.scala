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

    val actual = Source.fromFile(actualFile).getLines().map(line => {
      val strs = line.split("""\^\^""")
      assert(strs.length == 3, s"illegal $strs")
      (strs(0), if (strs(1).toInt >= 2) 1 else 0)
    })

    val predict = Source.fromFile(predictFile).getLines().map(line => {
      val strs = line.split("""\^\^""")
      assert(strs.length == 2, s"illegal $strs")
      (strs(0), strs(1).toInt)
    })
    var count = 0
    var hit = 0
    var miss = 0
    var zero = 0
    var one = 0
    actual.zip(predict).foreach(item => {
      assert(item._1._1 == item._2._1)
      if (item._1._2 == item._2._2) {
        hit += 1
      } else {
        miss += 1
      }

      if(item._1._2 == 0){
        zero += 1
      }else{
        one +=1
      }

      count += 1
    })
    println(s"count: $count hit: $hit miss: $miss  accuracy: ${hit/count.asInstanceOf[Double]}")
    println(s"JsonPath can cache: $one   JsonPath can't cache: $zero  ratio: ${one/count.asInstanceOf[Double]}")
  }
}
