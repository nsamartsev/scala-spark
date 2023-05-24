package schemas

import org.apache.spark.sql.types._

object MusicianSchema {

  val schema = StructType(
    Seq(
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("artists", StringType),
      StructField("duration_ms", DoubleType),
      StructField("release_date", DateType),
      StructField("year", IntegerType),
      StructField("acousticness", StringType),
      StructField("danceability", StringType),
      StructField("energy", StringType),
      StructField("instrumentalness", StringType),
      StructField("liveness", StringType),
      StructField("loudness", StringType),
      StructField("speechiness", StringType),
      StructField("tempo", StringType),
      StructField("valence", StringType),
      StructField("mode", StringType),
      StructField("key", StringType),
      StructField("popularity", DoubleType),
      StructField("explicit", StringType)
    )
  )

}
