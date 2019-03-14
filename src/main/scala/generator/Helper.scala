package generator

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ListBuffer


object Helper {
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

}
