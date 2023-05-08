package df

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Person extends App {
  val spark = SparkSession.builder()
    .appName("Playground")
    .master("local")
    .getOrCreate()

  val data = Seq(
    Row("Alice", 12),
    Row("Bob", 13)
  )

  val schema = Array(
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  )

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    StructType(schema)
  )

  df.show()
  df.printSchema()

  spark.stop()
}
