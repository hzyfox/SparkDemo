package generator

/**
  * create with generator
  * USER: husterfox
  */
object Test {
  def main(args: Array[String]): Unit = {
    val seq = Seq(1, 2, 3)
    filter(test(_)(2))
  }

  def filter(builder: Int => test): Unit = {
    println(builder(2))
  }

  case class test(a: Int)(c: Int)

}
