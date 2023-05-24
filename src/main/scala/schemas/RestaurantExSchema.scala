package schemas

import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.types._

object RestaurantExSchema {
  val schema = StructType(Seq(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("opened", StringType),
    StructField("photos_url", StringType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      ))
    )
  ))

}
