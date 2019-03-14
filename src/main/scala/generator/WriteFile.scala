package generator

import java.io.{BufferedWriter, FileWriter}

import scala.collection.mutable.ListBuffer


object WriteFile {
  def writeSingleFile(fileName: String, container: ListBuffer[ListBuffer[(String, String, String)]], append: Boolean = false): Unit = {
    val stringBuilder = new StringBuilder
    val writer = new BufferedWriter(new FileWriter(fileName, append))
    for (buffer <- container) {
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
    }
    writer.close()
  }

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

