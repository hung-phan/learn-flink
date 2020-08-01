import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object AverageProfitTumblingWindow extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

  // get input data
  val data = env.socketTextStream("localhost", 9999)

  val profitPerMonth = data
    .map { row =>
      val cols = row.split(",")

      (cols(1), cols(2), cols(3), cols(4).toInt, 1)
    }
    .keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
    .reduce { (curr, prev) =>
      (curr._1, curr._2, curr._3, prev._4 + curr._4, prev._5 + curr._5)
    }
    .map { input =>
      (input._1, input._4 * 1.0 / input._5)
    }

  // execute and print result
  profitPerMonth.print()

  env.execute("Avg Profit Per Month")
}
