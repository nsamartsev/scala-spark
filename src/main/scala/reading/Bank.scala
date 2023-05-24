package reading

import org.apache.spark.sql.functions.{asc, col}
import utils.{Constants, Context}

object Bank extends App with Context {

  var df = spark.read
    .options(
      Map("header" -> "true", "delimiter" -> ";")
    )
    .csv(Constants.BankCsvPath)


  /**
   * Task 1
   */
  //  df.select("*")
  //  .groupBy("age")
  //  .count()
  //  .show(5)

  /**
   * Task 2
   */
  //  df.select("age")
  //    .groupBy("age").count().orderBy(desc("count"))
  //    .show(5)

  /**
   * Task 3
   */
//    df.select("age")
//      .groupBy("age").count().orderBy(asc("age"))
//      .show(5)

  /**
   * Task 4
   */
//  df.filter(col("age") > 30)
//    .select("age")
//    .groupBy("age").count().orderBy(asc("age"))
//    .show(5)


}
