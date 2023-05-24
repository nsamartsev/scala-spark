package reading

import com.google.common.collect.ImmutableMap
import schemas.StocksPriceSchema
import utils.{Constants, Context}

import org.apache.spark.sql.functions._

object StocksPrice extends App with Context {

  var df = spark.read
    .options(
      Map("header" -> "true", "delimiter" -> ",")
    )
    .schema(StocksPriceSchema.schema)
    .csv(Constants.StocksPriceFinalCsvPath)

  val count = df.count()

  df = df.na.drop()
//  df.na.fill(df.select(f.mean(df['open'])).collect()[0][0])
  df.na.replace("height", ImmutableMap.of(1.0, 2.0))

  println(s"qqq: ${df.count() - count}")

  df.select()

  //  df = df.withColumn("new_date_column", df.col("date"))
  //  df = df.withColumnsRenamed(Map("date"->"date_changed"))
  //  df = df.drop("date_changed")
  //  df.printSchema()
  //  df.show(3)
  //  println(s"qqq: ${df.count()}")

  // task
//  df = df.withColumn("new_volume", df.col("volume"))
//    .drop("symbol", "close", "volume", "adjusted", "market.cap", "exchange")
//    .withColumnsRenamed(
//      Map(
//        "high" -> "low_tmp",
//        "low" -> "high_tmp",
//        "low_tmp" -> "low",
//        "high_tmp" -> "high"
//      )
//    )
//    .withColumn("new_date", df.col("date"))
//    .drop("date")
//    .withColumnsRenamed(Map("new_date"->"date"))
//
//
//    df.show(3)

}
