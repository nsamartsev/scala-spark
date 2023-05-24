package reading

import org.apache.spark.sql.types._
import schemas.PersonSchema
import utils.{Context, InputData}

object Person extends App with Context {
  val df = spark.createDataFrame(
    sc.parallelize(InputData.persons),
    StructType(PersonSchema.schema)
  )

  df.show()
  df.printSchema()

  spark.stop()
}
