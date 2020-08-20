package window_example

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTypeWithWatermark extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val data = env.socketTextStream("localhost", 9090)

  val sum = data
    .map { input =>
      val words = input.split(",")

      (words(0).toLong, words(1))
    }
    .assignTimestampsAndWatermarks(
      new DemoWatermark()
    )
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce { (curr, prev) =>
      val value = prev._2.toInt + curr._2.toInt
      val t = new Timestamp(System.currentTimeMillis())

      (t.getTime, value.toString)
    }

  sum.writeAsText("tmp/window_data")

  env.execute("Window")

  class DemoWatermark extends AssignerWithPeriodicWatermarks[(Long, String)] {
    private val allowedLateTime = 3500 // 3.5s
    private var currentMaxTimestamp = 0L

    override def getCurrentWatermark: Watermark =
      new Watermark(currentMaxTimestamp - allowedLateTime)

    override def extractTimestamp(
        element: (Long, String),
        previousElementTimestamp: Long
    ): Long = {
      val timestamp = element._1

      currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp)

      timestamp
    }
  }
}
