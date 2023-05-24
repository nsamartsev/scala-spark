package utils

import org.apache.spark.sql.SparkSession

trait Context {
  lazy val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local[2]")
    .getOrCreate()

  lazy val sc = spark.sparkContext
}
