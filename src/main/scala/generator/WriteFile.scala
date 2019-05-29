package generator

import java.io.{BufferedWriter, FileWriter}
import BaseJsonPath.separator

import scala.collection.mutable.ListBuffer


object WriteFile {
  def writeSingleFile(fileName: String, container: ListBuffer[ListBuffer[(String, String, String)]], append: Boolean = false): Unit = {
    val stringBuilder = new StringBuilder
    val writer = new BufferedWriter(new FileWriter(fileName, append))
    for (buffer <- container) {
      for (row <- buffer) {
        stringBuilder.append(row._1)
        stringBuilder.append(separator)
        stringBuilder.append(row._2)
        stringBuilder.append(separator)
        stringBuilder.append(row._3)
        writer.write(stringBuilder.toString())
        writer.newLine()
        stringBuilder.clear()
      }
    }
    writer.close()
  }

  def writeSingleFileAndVerify(fileName: String, container: ListBuffer[ListBuffer[(String, String, String)]], append: Boolean = false, endDay:String = ""): Unit = {
    val stringBuilder = new StringBuilder
    val stringBuilder0 = new StringBuilder
    val writer = new BufferedWriter(new FileWriter(fileName, append))
    val verifyWriter = new BufferedWriter(new FileWriter(fileName + ".verify", append))
    for (buffer <- container) {
      for (row <- buffer) {
        if (row._3 == endDay) {
          stringBuilder0.append(row._1)
          stringBuilder0.append(separator)
          stringBuilder0.append(if(row._2 > "1") "1" else "0")
          verifyWriter.write(stringBuilder0.toString())
          verifyWriter.newLine()
          stringBuilder0.clear()
        } else {
          stringBuilder.append(row._1)
          stringBuilder.append(separator)
          stringBuilder.append(row._2)
          stringBuilder.append(separator)
          stringBuilder.append(row._3)
          writer.write(stringBuilder.toString())
          writer.newLine()
          stringBuilder.clear()
        }
      }
    }
    verifyWriter.close()
    writer.close()
  }
  @deprecated
  def writeSeparateFile(fileName: String, container: ListBuffer[ListBuffer[(String, String, String)]], append: Boolean = false): Unit = {
    val stringBuilder = new StringBuilder
    for (buffer <- container) {
      var writerCount = 0
      val writer = new BufferedWriter(new FileWriter("separateFile" + writerCount, append))
      writerCount += 1
      for (row <- buffer) {
        stringBuilder.append(row._1)
        stringBuilder.append(" ")
        stringBuilder.append(row._2)
        stringBuilder.append(" ")
        stringBuilder.append(row._3)
        writer.write(stringBuilder.toString())
        writer.newLine()
        stringBuilder.clear()
      }
      writer.close()
    }
  }
}

