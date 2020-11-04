import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object StateDemo extends App {
  class StatefulMap(var sum: ValueState[Long], var count: ValueState[Long])
      extends RichFlatMapFunction[(Long, String), Long] {

    def this() = {
      this(null, null)
    }

    override def flatMap(input: (Long, String), out: Collector[Long]): Unit = {
      count.update(count.value() + 1)
      sum.update(sum.value() + input._1)

      if (count.value() >= 10) {
        out.collect(sum.value())

        count.clear()
        sum.clear()
      }
    }

    override def open(parameters: Configuration): Unit = {
      sum = getRuntimeContext().getState(
        new ValueStateDescriptor[Long](
          "sum",
          TypeInformation.of(new TypeHint[Long] {})
        )
      )

      count = getRuntimeContext.getState(
        new ValueStateDescriptor[Long](
          "count",
          TypeInformation.of(new TypeHint[Long] {})
        )
      )
    }
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val data = env.socketTextStream("localhost", 9090)

  val sum = data
    .map { input =>
      val words = input.split(",")

      (words(0).toLong, words(1))
    }
    .keyBy(0)
    .flatMap(new StatefulMap())

  sum.writeAsText("tmp/state_demo")

  env.execute("StateDemo")
}
