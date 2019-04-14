package generator

/**
  * create with generator
  * USER: husterfox
  */
object PrintDays {

  def main(args: Array[String]): Unit = {
    val builder = new StringBuilder()
    val dayScope = Helper.getBetweenDatesByInterval("20181203", "20181231", 1)
    dayScope._1.foreach(day => builder.append(day + " "))
    println(builder.toString())
    builder.clear()
    dayScope._2.foreach(day => builder.append(day + " "))
    println(builder.toString())
  }

}
