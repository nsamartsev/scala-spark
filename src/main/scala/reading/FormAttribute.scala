package reading

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import utils.{Constants, Context}

object FormAttribute extends App with Context {
  spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("multiline", "true")
    .load(Constants.FormAttrJsonPath)
    .withColumn("sectionId", typedLit(123L))
    .withColumn("versioned", typedLit(false))
    .withColumn("tip",
      map(
        lit("description"), lit("текст подсказки"),
      )
    )
    .withColumn("settings",
      map(
        lit("size"), lit("half"),
      )
    )
    .write
    .mode(SaveMode.Overwrite)
    .option("lineSep", ",")
    .json("./src/main/resources/attr_out.json")
}
