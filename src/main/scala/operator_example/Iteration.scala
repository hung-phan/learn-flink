package operator_example

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Iteration extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val data: DataStream[Int] = env.fromElements(0)
  val iteration = data.iterate((stepFunction: DataStream[Int]) => {
    val feedback = stepFunction
      .filter(_ < 10)
      .map(_ + 1)
      .setParallelism(1)

    feedback.print()

    val output = stepFunction.filter(_ == 10)

    (feedback, output)
  })

  env.execute("Iteration")
}
