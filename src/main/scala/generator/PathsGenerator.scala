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
      "1" -> "0.8"
    )

    val occurProbabilityMap1: Map[String, String] = Map(
      "0" -> "0.1",
      "2" -> "0.9"
    )

    val occurProbabilityMap2: Map[String, String] = Map(
      "0" -> "0.1",
      "3" -> "0.9"
    )

    val randomBase = 10000
    val startDay = "20190302"
    val endDay = "20190309"
    val info0 = new ListBuffer[(String, Map[String, String], Int, String, String)]
    val info1 = new ListBuffer[(String, Map[String, String], Int, String, String)]
    val info2 = new ListBuffer[(String, Map[String, String], Int, String, String)]

    for (i <- 0 until 70) {
      info0 += ((namePath + i, occurProbabilityMap, randomBase, startDay, endDay))
    }
    for (i <- 0 until 20) {
      info1 += ((agePath + i, occurProbabilityMap1, randomBase, startDay, endDay))
    }
    for (i <- 0 until 10) {
      info2 += ((genderPath + i, occurProbabilityMap2, randomBase, startDay, endDay))
    }
    val buffers = new ListBuffer[(String, Map[String, String], Int, String, String)]
    buffers ++= info0
    buffers ++= info1
    buffers ++= info2
    generateSingleFile("verify.txt", buffers)
  }
}
