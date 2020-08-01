package window_example

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowType extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val data = env.socketTextStream("localhost", 9090)

  val sum = data
    .map { input =>
      val words = input.split(",")

      (words(0).toLong, words(1))
    }
    .assignTimestampsAndWatermarks(
      new AscendingTimestampExtractor[(Long, String)] {
        override def extractAscendingTimestamp(element: (Long, String)): Long =
          element._1
      }
    )
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce { (curr, prev) =>
      val value = prev._2.toInt + curr._2.toInt
      val t = new Timestamp(System.currentTimeMillis())

      (t.getTime, value.toString)
    }

  sum.writeAsText("tmp/window_data")

  env.execute("Window")
}
