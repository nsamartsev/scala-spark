package reading

import utils.{Context, InputData}

object Course extends App with Context {
  import spark.implicits._
  val coursesDF = InputData.courses.toDF("title", "duration (h)")
  coursesDF.show()
  coursesDF.printSchema()
}
