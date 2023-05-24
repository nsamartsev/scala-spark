package reading

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, when}
import utils.{Constants, Context}

object StocksPriceSQL extends App with Context {

  var df = spark.read
    .options(
      Map("header" -> "true", "delimiter" -> ",")
    )
    .csv(Constants.StocksPriceFinalCsvPath)

  /**
   * Select
   */
  df.select("sector").show(5)

  df.select("open", "close", "adjusted").show(5)


  /**
   * Filter
   */
  df.filter(df.col("date") >= "2020-01-01")
    .filter(df.col("date") <= "2020-01-31")
    .show(5)


  /**
   * Between
   */
  df.filter(df.col("adjusted").between(100.0, 500.0)).show()


  /**
   * When otherwise
   */
  val cond = when(col("adjusted") >= 200.0, 1).otherwise(0)
  df.withColumns(Map("open" -> cond, "close" -> cond))
    .show(6)


  /**
   * Like
   */
  df.select(
    col("sector")
      .rlike("^[B,C]")
      .alias("Колонка sector начинается с B или C")
  ).distinct().show()


  /**
   * GroupBy
   */
  df.select("industry", "open", "close", "adjusted")
    .groupBy("industry")
    .mean()
    .show()


  /**
   * Aggregation
   */
  df.filter(col("date") >= "2019-01-02")
    .filter(col("date") >= "2020-01-31")
    .groupBy("sector")
    .agg(
      functions.min("date").alias("C"),
      functions.max("date").alias("По"),

      functions.min("open").alias("Минимум при открытии"),
      functions.max("open").alias("Максимум при открытии"),
      functions.avg("open").alias("Среднее в open"),

      functions.min("close").alias("Минимум при закрытии"),
      functions.max("close").alias("Максимум при закрытии"),
      functions.avg("close").alias("Среднее в close"),

      functions.min("adjusted").alias("Скорректированный минимум"),
      functions.max("adjusted").alias("Скорректированный максимум"),
      functions.avg("adjusted").alias("Среднее в adjusted")

    )
    .show(truncate = false)
}
