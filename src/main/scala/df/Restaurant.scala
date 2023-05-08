package df

import org.apache.spark.sql.{Row, SparkSession}

object Restaurant extends App {
  val spark = SparkSession.builder()
    .appName("Read restaurants from file without schema")
    .master("local")
    .getOrCreate()

  val df = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("multiline", "true")
    .load("src/main/resources/1_df_files/restaurant.json")

  df.show(2)
  df.printSchema()

  val restaurantArray: Array[Row] = df.take(1)
  restaurantArray.foreach(println)
}
