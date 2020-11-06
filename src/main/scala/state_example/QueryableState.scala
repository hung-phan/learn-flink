package state_example

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.{
  ConfigConstants,
  Configuration,
  QueryableStateOptions
}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object QueryableState extends App {
  val config = new Configuration()

  config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)
  config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1)
  config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1)
  config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val data = env.socketTextStream("localhost", 9090)

  val sum = data
    .map { line =>
      val words = line.split(",")

      (words(0).toLong, words(1))
    }
    .keyBy(0)
    .flatMap(new StatefulMap)

  sum.writeAsText("tmp/queryable_state")

  env.execute("Queryable State")

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
      val sumDescriptor = new ValueStateDescriptor[Long](
        "sum",
        TypeInformation.of(new TypeHint[Long] {})
      )
      val countDescriptor = new ValueStateDescriptor[Long](
        "count",
        TypeInformation.of(new TypeHint[Long] {})
      )

      sumDescriptor.setQueryable("sum-query")
      countDescriptor.setQueryable("count-query")

      sum = getRuntimeContext().getState(sumDescriptor)
      count = getRuntimeContext.getState(countDescriptor)
    }
  }
}
