package state_example

import org.apache.flink.api.common.state.{
  MapStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object BroadcastState extends App {
  val excludeEmpDescriptor = new MapStateDescriptor[String, String](
    "excludeEmploy",
    TypeInformation.of(new TypeHint[String] {}),
    TypeInformation.of(new TypeHint[String] {})
  )
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val excludeEmp = env.socketTextStream("localhost", 9090)
  val excludeEmpBroadcast = excludeEmp.broadcast(excludeEmpDescriptor)
  val employees = env
    .readTextFile(
      getClass.getResource("/state_example/broadcast.txt").getPath
    )
    .map { row => (row.split(",")(3), row) }
    .keyBy(0)
    .connect(excludeEmpBroadcast)
    .process(new ExcludeEmp())

  class ExcludeEmp
      extends KeyedBroadcastProcessFunction[
        String,
        (String, String),
        String,
        (String, Int)
      ] {

    @transient private var countState: ValueState[Int] = _

    override def open(conf: Configuration): Unit = {
      countState = getRuntimeContext.getState(
        new ValueStateDescriptor[Int](
          "countState",
          TypeInformation.of(new TypeHint[Int] {})
        )
      )
    }

    override def processElement(
        value: (String, String),
        ctx: KeyedBroadcastProcessFunction[
          String,
          (String, String),
          String,
          (String, Int)
        ]#ReadOnlyContext,
        out: Collector[(String, Int)]
    ): Unit = {
      val currCount = countState.value()

      // get card_id of current transaction
      val cId = value._2.split(",")(0)

      val excludedVal = ctx
        .getBroadcastState(excludeEmpDescriptor)
        .immutableEntries()
        .asScala
        .find { cardEntry => cId == cardEntry.getKey }

      if (excludedVal.isEmpty) {
        // dept, current sum
        countState.update(currCount + 1)
        out.collect((value._1, currCount + 1))
      }
    }

    override def processBroadcastElement(
        value: String,
        ctx: KeyedBroadcastProcessFunction[
          String,
          (String, String),
          String,
          (String, Int)
        ]#Context,
        out: Collector[(String, Int)]
    ): Unit = {
      val id = value.split(",")(0)
      ctx.getBroadcastState(excludeEmpDescriptor).put(id, value)
    }
  }

  employees.writeAsText("tmp/broadcast")

  env.execute("Broadcast Example")
}
