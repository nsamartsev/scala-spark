package reading

import utils.{Constants, Context}
import org.apache.spark.sql.Row

object Restaurant extends App with Context {
  val df = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("multiline", "true")
    .load(Constants.RestaurantJsonPath)

  df.show(2)
  df.printSchema()

  val restaurantArray: Array[Row] = df.take(1)
  restaurantArray.foreach(println)
}
