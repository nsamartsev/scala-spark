package df

import org.apache.spark.sql.SparkSession

object Course extends App {
  val spark = SparkSession.builder()
    .appName("Playground")
    .master("local")
    .getOrCreate()

  val courses = Seq(
    ("Scala", 22),
    ("Spark", 30)
  )

  import spark.implicits._

  val coursesDF = courses.toDF("title", "duration (h)")

  coursesDF.show()
  coursesDF.printSchema()
}
