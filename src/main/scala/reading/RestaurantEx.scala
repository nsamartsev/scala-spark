package reading

import utils.{Constants, Context}
import schemas.RestaurantExSchema

object RestaurantEx extends App with Context {
  val df = spark.read
    .option("multiline", "true")
    .schema(RestaurantExSchema.schema)
    .json(Constants.RestaurantExJsonPath)

  df.show()
}
