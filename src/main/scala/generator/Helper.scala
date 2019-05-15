package generator

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks


object Helper {

  def checkDay(startDay: String, endDay: String) = {
    val dayFormat = new SimpleDateFormat("yyyyMMDD")
    try {
      val start = dayFormat.parse(startDay)
      val end = dayFormat.parse(endDay)
      val tempStart = Calendar.getInstance()
      tempStart.setTime(start)
      val tempEnd = Calendar.getInstance()
      tempEnd.setTime(end)
      if (tempEnd.before(tempStart)) {
        System.err.println(s"endDay: ${endDay} can't before startDay: ${startDay}")
        System.exit(-1)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(-1)
    }
  }

  def getBetweenDates(start: String, end: String): List[String] = {
    val startData = new SimpleDateFormat("yyyyMMdd").parse(start); //定义起始日期
    val endData = new SimpleDateFormat("yyyyMMdd").parse(end); //定义结束日期

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var buffer = new ListBuffer[String]

    val tempStart = Calendar.getInstance()
    tempStart.setTime(startData)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)
    if (tempStart.after(tempEnd)) {
      throw new IllegalArgumentException(s"startDay $start after endDay $end")
    }

    while (tempStart.before(tempEnd)) {
      // result.add(dateFormat.format(tempStart.getTime()))
      buffer += dateFormat.format(tempStart.getTime)
      tempStart.add(Calendar.DAY_OF_YEAR, 1)
    }
    buffer += dateFormat.format(endData.getTime)
    buffer.toList
  }

  def getBetweenDatesByInterval(start: String, end: String, interval: Int): (List[String], List[String]) = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val startDay = dateFormat.parse(start)
    val endDay = dateFormat.parse(end)
    val startBuffer = new ListBuffer[String]
    val endBuffer = new ListBuffer[String]

    val tempStart = Calendar.getInstance()
    tempStart.setTime(startDay)
    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endDay)

    if (tempStart.after(tempEnd)) {
      throw new IllegalArgumentException(s"startDay $start after endDay $end")
    }

    while (!tempStart.after(tempEnd)) {
      startBuffer += dateFormat.format(tempStart.getTime)
      Breaks.breakable {
        for (_ <- 0 until interval) {
          tempStart.add(Calendar.DAY_OF_YEAR, 1)
          if (tempStart.after(tempEnd)) {
            tempStart.add(Calendar.DAY_OF_YEAR, -1)
            endBuffer += dateFormat.format(tempStart.getTime)
            tempStart.add(Calendar.DAY_OF_YEAR, 1)
            Breaks.break
          }
        }
        if (!tempStart.after(tempEnd)) {
          endBuffer += dateFormat.format(tempStart.getTime)
        }
        tempStart.add(Calendar.DAY_OF_YEAR, 1);
      }
    }
    (startBuffer.toList, endBuffer.toList)
  }

  def main(args: Array[String]): Unit = {
    println(getBetweenDatesByInterval("20190101", "20190105", 2))
  }


}
