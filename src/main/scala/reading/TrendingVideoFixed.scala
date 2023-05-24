package reading

import org.apache.spark.sql.types.StructType
import preprocessing.TrendingVideosFixed
import schemas.TrendingVideoSchema
import utils.Context

object TrendingVideoFixed extends App with Context {

  val df = spark.createDataFrame(
    sc.parallelize(TrendingVideosFixed.inputWithTimestamp),
    StructType(TrendingVideoSchema.schemaWithTime)
  )

  df.show()
}
