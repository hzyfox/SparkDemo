package generator

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ListBuffer

object OccurProbability {
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
}

object BaseJsonPath {
//  val namePath = "name"
//  val agePath = "age"
//  val genderPath = "person.gender"
  val namePath = "id"
  val agePath = "name"
  val genderPath = "age"
  val separator = "^^"
  //for random
  val randomBase = 10000
}

object PathsGenerator {

  def generateSingleFile(fileName: String,
                         Path2Num: Seq[(String, Integer)],
                         occurProbabilityMaps: Seq[Map[String, String]],
                         startDay: String,
                         endDay: String,
                         randomBase: Int = 10000): Unit = {
    //我们每次最多1000条basepath
    val maxGenerate: Int = 1000
    for (i <- Path2Num.indices) {
      val (basePath, num) = Path2Num(i)
      var tmpBuffer = new ListBuffer[ListBuffer[(String, String, String)]]()
      for (j <- 0 until num) {
        tmpBuffer += SinglePathGenerator.generator(basePath + j, occurProbabilityMaps(i), randomBase, startDay, endDay)
        if (j != 0 && j % maxGenerate == 0) {
          WriteFile.writeSingleFile(fileName, tmpBuffer, append = true)
          tmpBuffer.clear()
        }
      }
      if (tmpBuffer.nonEmpty) {
        WriteFile.writeSingleFile(fileName, tmpBuffer, append = true)
        tmpBuffer.clear()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    import BaseJsonPath._
    import OccurProbability._

    if (args.length < 6) {
      System.err.println("please specify <startDat eg. 20190101> <endDay eg. 20190131> <NamePathNumber eg: 100> <AgePathNumber eg: 100> <genderPathNumber eg: 100> <outPutFile eg: train-zhang.txt>")
      System.exit(-1)
    }

    val startDay = args(0)
    val endDay = args(1)
    val namePathNum = Integer.valueOf(args(2))
    val agePathNum = Integer.valueOf(args(3))
    val genderPathNum = Integer.valueOf(args(4))
    val outputFile = args(5)
    Helper.checkDay(startDay, endDay)
    val basePathGroup = Seq(namePath, agePath, genderPath)
    val basePathNum = Seq(namePathNum, agePathNum, genderPathNum)
    val occurProbabilityMaps = Seq(occurProbabilityMap, occurProbabilityMap1, occurProbabilityMap2)
    val path2Num = basePathGroup.zip(basePathNum)
    generateSingleFile(outputFile, path2Num, occurProbabilityMaps, startDay, endDay, randomBase = 10000)
  }


}
