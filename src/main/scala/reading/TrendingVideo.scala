package reading

import org.apache.spark.sql.types.StructType
import schemas.TrendingVideoSchema
import utils.{Context, InputData}

object TrendingVideo extends App with Context {

  val df = spark.createDataFrame(
    sc.parallelize(InputData.videos),
    StructType(TrendingVideoSchema.schema)
  )
  df.show()

}
