import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WebsiteAnalysis extends App {
  //  ip_data.txt file is of following schema
  //
  //  ## user_id,network_name,user_IP,user_country,website, Time spent before next click
  //
  //  For every 10 second find out for US country
  //
  //  a.) total number of clicks on every website in separate file
  //
  //  b.) the website with maximum number of clicks in separate file.
  //
  //  c.) the website with minimum number of clicks in separate file.
  //
  //  d.) Calculate number of distinct users on every website in separate file.
  //
  //  e.) Calculate the average time spent on website by users.

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

  val text = env.readTextFile(
    getClass().getResource("ip-data.txt").getPath,
    "UTF-8"
  )

  case class IPData(
      userID: String,
      networkName: String,
      userIP: String,
      userCountry: String,
      website: String,
      timeSpent: Long
  )

  object IPData {
    def apply(row: String): IPData = {
      val cols = row.split(",")

      IPData(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5).toLong)
    }
  }

  val stream = text.map { IPData(_) }

  val totalClicksGroupedByWebsite = stream
    .map { ipData => (ipData.website, 1) }
    .keyBy(0)
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .sum(1)

  totalClicksGroupedByWebsite.print()

  val websiteWithMaximumNumOfClicks = totalClicksGroupedByWebsite
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
    .maxBy(1)

  websiteWithMaximumNumOfClicks.print()

  val websiteWithMinimumNumOfClicks = totalClicksGroupedByWebsite
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
    .minBy(1)

  websiteWithMinimumNumOfClicks.print()

  val websiteGroupedByUserAccess = stream
    .map { ipData => (ipData.website, ipData.userID, 1) }
    .keyBy(0, 1)
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .sum(2)

  val distinctUserPerWebsite = websiteGroupedByUserAccess
    .map { data => (data._1, 1) }
    .keyBy(0)
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .sum(1)

  distinctUserPerWebsite.print()

  val averageTimeSpentOnWebsiteByUser = stream
    .map { ipData => (ipData.userID, ipData.timeSpent, 1) }
    .keyBy(0)
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .reduce { (data1, data2) =>
      (data1._1, data1._2 + data2._2, data1._3 + data2._3)
    }
    .map { data => (data._1, data._2 * 1.0 / data._3) }

  averageTimeSpentOnWebsiteByUser.print()

  env.execute("WebsiteAnalysis")
}
