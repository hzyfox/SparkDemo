package generator

import scala.collection.mutable.ListBuffer
import scala.util.Random

object SinglePathGenerator {


  def generator(jsonPath: String,
                occurProbabilityMap: Map[String, String],
                randomBase: Int,
                startDay: String,
                endDay: String): ListBuffer[(String, String, String)] = {

    assert(occurProbabilityMap.foldLeft(0f)((a, b) => a + b._2.toFloat) == 1.0000, "probability should be 1")
    val random = new Random()
    val daysList: List[String] = Helper.getBetweenDates(startDay, endDay)
    //JsonPath    OccurNumber     TimeStamp
    val resultList: ListBuffer[(String, String, String)] = new ListBuffer[(String, String, String)]
    var start = 0
    val dataScopeList = occurProbabilityMap.toList.map(data => {
      val end = start + (randomBase * data._2.toFloat).asInstanceOf[Int]
      val dataScope = (data._1, end)
      start = end
      dataScope
    })
    for (day <- daysList) {
      val seed = random.nextInt(randomBase)
      resultList += ((jsonPath, dataScopeList.find(scope => seed < scope._2).get._1.toString, day))
    }
    resultList
  }




}
