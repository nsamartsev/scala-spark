package reading

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import utils.{Constants, Context}

object FormAttribute extends App with Context {
  var df = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("multiline", "true")
    .load(Constants.FormAttrJsonPath)

  df = df.withColumn("sectionId", typedLit(123L))
  df = df.withColumn("versioned", typedLit(false))
  df = df.withColumn("tip",
    map(
      lit("description"), lit("текст подсказки"),
    )
  )
  df = df.withColumn("settings",
    map(
      lit("size"), lit("half"),
    )
  )


  df.write
    .mode(SaveMode.Overwrite)
    .option("lineSep", ",")
    .json("./src/main/resources/attr_out.json")
}
