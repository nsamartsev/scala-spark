package df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object RestaurantEx extends App {

  // TODO -- Make file in your environment
  val DataJsonPath = "src/main/resources/1_df_files/restaurant_ex.json"

  val spark = SparkSession.builder()
    .appName("Read data from restaurant_ex.json with defined schema")
    .master("local")
    .getOrCreate()

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
      )))
  ))

  val df = spark.read
    .option("multiline", "true")
    .schema(schema)
    .json(DataJsonPath)

  df.show()

}
