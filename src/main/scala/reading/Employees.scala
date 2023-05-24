package reading

import org.apache.spark.sql.functions.avg
import utils.{Constants, Context}


/**
 * File employees.csv contains:
 *
 * salary,department
 * 10000,Developers
 * 20000,Operations
 * 30000,Admins
 * 40000,Analysts
 * 40000,Admins
 */

object Employees extends App with Context {

  var employeesDf = spark.read
    .options(
      Map(
        "header" -> "true",
        "inferSchema" -> "true",
      )
    )
    .csv(Constants.employeesCsvPath)


  val avgSalaryDf = employeesDf
    .groupBy("Department")
    .agg(avg("salary") as "Average department's salary")

  avgSalaryDf.show(10, false)

  avgSalaryDf.explain()

}
