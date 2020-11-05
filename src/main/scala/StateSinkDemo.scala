import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ArrayBuffer

object StateSinkDemo extends App {
  class Sink(val threshold: Int)
      extends SinkFunction[(String, Int)]
      with CheckpointedFunction {
    @transient var checkPointedState: Option[ListState[(String, Int)]] = None

    private val bufferedElements = ArrayBuffer[(String, Int)]()

    @throws(classOf[Exception])
    def invoke(
        value: (String, Int),
        context: SinkFunction.Context[(String, Int)]
    ) = {
      bufferedElements += value

      if (bufferedElements.length >= threshold) {
        for { elem <- bufferedElements } {
          println("Send it to the sink", elem)
        }

        bufferedElements.clear()
      }
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      checkPointedState.foreach { state =>
        state.clear()

        for { elem <- bufferedElements } {
          state.add(elem)
        }
      }
    }

    override def initializeState(
        context: FunctionInitializationContext
    ): Unit = {
      checkPointedState = Some(
        context
          .getOperatorStateStore()
          .getListState(
            new ListStateDescriptor[(String, Int)](
              "bufferedElements",
              TypeInformation.of(new TypeHint[(String, Int)] {})
            )
          )
      )

      if (context.isRestored) {
        checkPointedState.foreach { state =>
          state.get().forEach { elem =>
            bufferedElements += elem
          }
        }
      }
    }
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment()

  // start a checkpoint every 1000 ms
  env.enableCheckpointing(1000)

  val checkpointConfig = env.getCheckpointConfig()

  // to set minimum progress time to happen between checkpoints
  checkpointConfig.setMinPauseBetweenCheckpoints(500)

  // checkpoints have to complete within 10000 ms or are discarded
  checkpointConfig.setCheckpointTimeout(10000)

  // set mode to exactly-once (this is the default)
  checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // AT_LEAST_ONCE

  // allow ony one checkpoint to be in progress at the same time
  checkpointConfig.setMaxConcurrentCheckpoints(1)

  // enable externalized checkpoints which are retained after job cancellation
  checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) // DELETE_ON_CANCELLATION

  // number of restart attempts, delay between attempts
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10))
}
