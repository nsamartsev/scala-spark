package preprocessing

import org.apache.spark.sql.Row
import utils.InputData

import java.sql.Timestamp
import java.time.Instant

object TrendingVideosFixed {

  val inputWithTimestamp = InputData.videos2
    .map {
      case (videoId, publishedAt, channelId, trendingDate) => Row(
        videoId,
        Timestamp.from(Instant.parse(publishedAt)),
        channelId,
        Timestamp.from(Instant.parse(trendingDate)),
      )
    }

}
