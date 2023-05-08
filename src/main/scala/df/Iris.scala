package df

import org.apache.spark.sql.{Row, SparkSession}

object Iris extends App {

  private val IrisJsonPath = "src/main/resources/1_df_files/iris.json"

  val spark = SparkSession.builder()
    .appName("Read restaurants from file without schema")
    .master("local")
    .getOrCreate()


  val irisDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("multiline", "true") //TODO  Since apache spark version 2.3
    .load(IrisJsonPath)


//  irisDF.take(3).foreach(println)

  val irisArray: Array[Row] = irisDf.take(3)
  irisArray.foreach(println)

}
