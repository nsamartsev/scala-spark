package df

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object TrendingVideo extends App {
  val input = Seq(
    Row(
      "s9FH4rDMvds",
      "2020-08-11T22:21:49Z",
      "UCGfBwrCoi9ZJjKiUK8MmJNw",
      "2020-08-12T00:00:00Z"
    ),
    Row(
      "kZxn-0uoqV8",
      "2020-08-11T14:00:21Z",
      "UCGFNp4Pialo9wjT9Bo8wECA",
      "2020-08-12T00:00:00Z"
    ),
    Row(
      "QHpU9xLX3nU",
      "2020-08-10T16:32:12Z",
      "UCAuvouPCYSOufWtv8qbe6wA",
      "2020-08-12T00:00:00Z"
    )
  )

  val spark = SparkSession.builder()
    .appName("Trending Videos DataFrame")
    .master("local")
    .getOrCreate()

  val schema = Array(
    StructField("videoId", StringType),
    StructField("publishedAt", StringType),
    StructField("channelId", StringType),
    StructField("trendingDate", StringType)
  )

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(input),
    StructType(schema)
  )

  df.show()
}
