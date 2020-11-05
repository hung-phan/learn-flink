package operator_example

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Aggregation extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // get input data
  val data = env.readTextFile(getClass.getResource("avg1").getPath)

  val mapped = data.map { row =>
    val cols = row.split(",")

    (cols(1), cols(2), cols(3), cols(4).toInt, 1)
  }

  mapped
    .keyBy(0)
    .sum(3)
    .map(data => data.productIterator.mkString(","))
    .addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("tmp/aggregation/sum"),
          new SimpleStringEncoder[String]("UTF-8")
        )
        .build()
    )

  mapped
    .keyBy(0)
    .min(3)
    .map(data => data.productIterator.mkString(","))
    .addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("tmp/aggregation/min"),
          new SimpleStringEncoder[String]("UTF-8")
        )
        .build()
    )

  mapped
    .keyBy(0)
    .minBy(3)
    .map(data => data.productIterator.mkString(","))
    .addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("tmp/aggregation/minBy"),
          new SimpleStringEncoder[String]("UTF-8")
        )
        .build()
    )

  mapped
    .keyBy(0)
    .max(3)
    .map(data => data.productIterator.mkString(","))
    .addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("tmp/aggregation/max"),
          new SimpleStringEncoder[String]("UTF-8")
        )
        .build()
    )

  mapped
    .keyBy(0)
    .maxBy(3)
    .map(data => data.productIterator.mkString(","))
    .addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("tmp/aggregation/maxBy"),
          new SimpleStringEncoder[String]("UTF-8")
        )
        .build()
    )

  env.execute("Avg Profit Per Month")
}
