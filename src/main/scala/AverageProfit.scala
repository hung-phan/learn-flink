import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object AverageProfit extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // get input data
  val data = env.readTextFile(getClass.getResource("avg").getPath)

  val profitPerMonth = data
    .map { row =>
      val cols = row.split(",")

      (cols(1), cols(2), cols(3), cols(4).toInt, 1)
    }
    .keyBy(0)
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
