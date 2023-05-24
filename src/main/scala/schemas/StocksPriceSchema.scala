package schemas

import org.apache.spark.sql.types._

object StocksPriceSchema {
  val schema = StructType(
    Seq(
      StructField("_c0", IntegerType),
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("open", DoubleType),
      StructField("high", DoubleType),
      StructField("low", DoubleType),
      StructField("close", DoubleType),
      StructField("volume", IntegerType),
      StructField("adjusted", DoubleType),
      StructField("market.cap", StringType),
      StructField("sector", StringType),
      StructField("industry", StringType),
      StructField("exchange", StringType),
    )
  )
}
