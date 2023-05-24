package reading

import org.apache.spark.sql.{Column, functions}
import schemas.MusicianSchema
import utils.{Constants, Context}

object Musician extends App with Context {

  var df = spark.read
    .options(
      Map(
        "header" -> "true",
        "delimiter" -> ",",
        "escape" -> "\""
      )
    )
    .schema(MusicianSchema.schema)
    .csv(Constants.MusicianCsvPath)

  /**
   * Task 1
   */
  //  df.select(
  //    col("year"),
  //    col("acousticness"),
  //    col("danceability"),
  //    col("energy"),
  //    col("speechiness"),
  //    col("liveness"),
  //    col("valence"),
  //  )
  //    .groupBy("year")
  //    .agg(
  //      round(avg("acousticness"), 2) as "acousticness",
  //      round(avg("danceability"), 2) as "danceability",
  //      round(avg("energy"), 2) as "energy",
  //      round(avg("liveness"), 2) as "liveness",
  //      round(avg("speechiness"), 2) as "speechiness",
  //      round(avg("valence"), 2) as "valence"
  //    )
  //    .orderBy(asc("year"))
  //    .show(5)

  /**
   * Task 2
   */
  //  val count = df.filter(col("artists").contains("Sergei Rachmaninoff"))
  //    .filter(col("year") >= 1951)
  //    .count()
  //  println(s"Sergei Rachmaninoff titles after 1951: $count")

  /**
   * Task 3
   */
  import org.apache.spark.sql.functions._

  val toArray = udf[Array[String], String](_.split(", "))

  def replace(c: Column) = functions.regexp_replace(c, "(?:\\[\\'|\\'|\\])", "")

  val featureDf = df.withColumn("new_artists", toArray(df("artists")))
    .withColumn("new", explode(col("new_artists")))

  val newdf = featureDf.withColumn("artist", replace(col("new")))

  newdf
    .groupBy("artist")
    .mean("popularity")
    .filter(count("artists") > 200)
    .orderBy(desc("avg(popularity)"))
    .show(5)

}
