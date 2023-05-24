package reading

import utils.{Constants, Context}
import org.apache.spark.sql.Row

object Iris extends App with Context {
  val irisDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("multiline", "true")
    .load(Constants.IrisJsonPath)

  val irisArray: Array[Row] = irisDf.take(3)
  irisArray.foreach(println)
}
