package schemas

import org.apache.spark.sql.types._

object PersonSchema {

  val schema = Array(
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  )

}
