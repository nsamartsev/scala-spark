package reading

import org.apache.spark.sql.functions.{col, udf}
import utils.{Constants, Context}

object Numbers extends App with Context {

  val numbersDf = spark.read
    .options(
      Map(
        "header" -> "true",
        "inferSchema" -> "true",
      )
    )
    .csv(Constants.numbersCsvPath)

  def getSquare: String => Int = (num: String) =>
    num.split("_")(1).toInt


  def getSquareUdf = udf(getSquare)

  val squaresDF = numbersDf.select(
    col("id"),
    getSquareUdf(col("number") as "number")
  )

  squaresDF.show(5)

  squaresDF.explain(mode = "codegen")

}
