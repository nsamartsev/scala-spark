package schemas

import org.apache.spark.sql.types.{StringType, StructField, TimestampType}

object TrendingVideoSchema {

  val schema = Array(
    StructField("videoId", StringType),
    StructField("publishedAt", StringType),
    StructField("channelId", StringType),
    StructField("trendingDate", StringType)
  )

  val schemaWithTime = Array(
    StructField("videoId", StringType),
    StructField("publishedAt", TimestampType),
    StructField("channelId", StringType),
    StructField("trendingDate", TimestampType)
  )

}
