package df

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Timestamp
import java.time.Instant

object TrendingVideoFixed extends App {
  val input = Seq(
    (
      "s9FH4rDMvds",
      "2020-08-11T22:21:49Z",
      "UCGfBwrCoi9ZJjKiUK8MmJNw",
      "2020-08-12T00:00:00Z"
    ),
    (
      "kZxn-0uoqV8",
      "2020-08-11T14:00:21Z",
      "UCGFNp4Pialo9wjT9Bo8wECA",
      "2020-08-12T00:00:00Z"
    ),
    (
      "QHpU9xLX3nU",
      "2020-08-10T16:32:12Z",
      "UCAuvouPCYSOufWtv8qbe6wA",
      "2020-08-12T00:00:00Z"
    )
  )

  val inputWithTimestamp = input
    .map {
      case (videoId, publishedAt, channelId, trendingDate) => Row(
          videoId,
          Timestamp.from(Instant.parse(publishedAt)),
          channelId,
          Timestamp.from(Instant.parse(trendingDate)),
        )
      }

  val spark = SparkSession.builder()
    .appName("Trending Videos DataFrame")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  val schema = Array(
    StructField("videoId", StringType),
    StructField("publishedAt", TimestampType),
    StructField("channelId", StringType),
    StructField("trendingDate", TimestampType)
  )

  val df = spark.createDataFrame(
    sc.parallelize(inputWithTimestamp),
    StructType(schema)
  )

  df.show()
}
