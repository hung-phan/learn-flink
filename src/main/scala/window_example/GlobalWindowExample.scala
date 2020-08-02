package window_example

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

object GlobalWindowExample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

  val data = env.socketTextStream("localhost", 9090)

  val profitPerMonth = data
    .map { row =>
      val cols = row.split(",")

      (cols(1), cols(2), cols(3), cols(4).toInt, 1)
    }
    .keyBy(0)
    .window(GlobalWindows.create())
    .trigger(CountTrigger.of(5))
    .reduce { (curr, prev) =>
      (curr._1, curr._2, curr._3, prev._4 + curr._4, prev._5 + curr._5)
    }

  profitPerMonth.print()

  env.execute("Global window example")
}
