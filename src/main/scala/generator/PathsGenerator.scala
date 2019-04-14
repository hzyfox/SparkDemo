package generator

import scala.collection.mutable.ListBuffer

object PathsGenerator {

  def generateSingleFile(fileName: String, paths: ListBuffer[(String, Map[String, String], Int, String, String)], append: Boolean = false): Unit = {
    val result = new ListBuffer[ListBuffer[(String, String, String)]]()
    for (buffer <- paths) {
      result += SinglePathGenerator.generator(buffer._1, buffer._2, buffer._3, buffer._4, buffer._5)
    }
    WriteFile.writeSingleFile(fileName, result, append)
  }

  def generateSeparateFile(fileName: String, paths: ListBuffer[(String, Map[String, String], Int, String, String)], append: Boolean = false): Unit = {
    val result = new ListBuffer[ListBuffer[(String, String, String)]]()
    for (buffer <- paths) {
      result += SinglePathGenerator.generator(buffer._1, buffer._2, buffer._3, buffer._4, buffer._5)
    }
    WriteFile.writeSeparateFile(fileName, result, append)
  }

  def main(args: Array[String]): Unit = {
    val namePath = "name"
    val agePath = "age"
    val genderPath = "person.gender"
    val occurProbabilityMap: Map[String, String] = Map(
      "0" -> "0.2",
      "1" -> "0.6",
      "2" -> "0.2"
    )

    val occurProbabilityMap1: Map[String, String] = Map(
      "0" -> "0.1",
      "2" -> "0.4",
      "3" -> "0.4",
      "4" -> "0.1"
    )

    val occurProbabilityMap2: Map[String, String] = Map(
      "0" -> "0.1",
      "5" -> "0.2",
      "3" -> "0.5",
      "1" -> "0.2"
    )

    val randomBase = 10000
    val startDay = "20190101"
    val endDay = "20190131"
    val info0 = new ListBuffer[(String, Map[String, String], Int, String, String)]
    val info1 = new ListBuffer[(String, Map[String, String], Int, String, String)]
    val info2 = new ListBuffer[(String, Map[String, String], Int, String, String)]

    for (i <- 0 until 250) {
      info0 += ((namePath + i, occurProbabilityMap, randomBase, startDay, endDay))
    }
    for (i <- 0 until 200) {
      info1 += ((agePath + i, occurProbabilityMap1, randomBase, startDay, endDay))
    }
    for (i <- 0 until 200) {
      info2 += ((genderPath + i, occurProbabilityMap2, randomBase, startDay, endDay))
    }
    val buffers = new ListBuffer[(String, Map[String, String], Int, String, String)]
    buffers ++= info0
    buffers ++= info1
    buffers ++= info2
    generateSingleFile("train-zhang.txt", buffers)
  }
}
