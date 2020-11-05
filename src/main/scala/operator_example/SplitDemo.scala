package operator_example

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{
  OutputTag,
  StreamExecutionEnvironment
}
import org.apache.flink.util.Collector

object SplitDemo extends App {
  // set up the execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // make parameters available in web interfaces
  env.getConfig.setGlobalJobParameters(ParameterTool.fromArgs(args))

  val data = env.readTextFile(getClass.getResource("oddeven").getPath)
  val oddOutputTag = OutputTag[Int]("odd-output")
  val evenOutputTag = OutputTag[Int]("even-output")

  val mainDataStream = data
    .map(_.toInt)
    .process(
      (
          value: Int,
          ctx: ProcessFunction[Int, Int]#Context,
          out: Collector[Int]
      ) => {
        // emit data to regular output
        out.collect(value)

        ctx.output(if (value % 2 == 0) evenOutputTag else oddOutputTag, value)
      }
    )

  val oddOutputStream =
    mainDataStream
      .getSideOutput(oddOutputTag)
      .map(_.toString)
      .addSink(
        StreamingFileSink
          .forRowFormat(
            new Path("tmp/split/odd"),
            new SimpleStringEncoder[String]("UTF-8")
          )
          .build()
      )

  val evenOutputStream =
    mainDataStream
      .getSideOutput(evenOutputTag)
      .map(_.toString)
      .addSink(
        StreamingFileSink
          .forRowFormat(
            new Path("tmp/split/even"),
            new SimpleStringEncoder[String]("UTF-8")
          )
          .build()
      )

  env.execute("Split")
}
