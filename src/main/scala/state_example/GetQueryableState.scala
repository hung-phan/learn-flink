package state_example

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.queryablestate.client.QueryableStateClient

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object GetQueryableState extends App {
  val client = new QueryableStateClient("localhost", 9069)
  val descriptor = new ValueStateDescriptor[Long](
    "sum",
    TypeInformation.of(new TypeHint[Long] {})
  )

  val key = 1L

  val result = client
    .getKvState(
      JobID.fromHexString("abcdefgh"),
      "sum-query",
      key,
      TypeInformation.of(new TypeHint[Long] {}),
      descriptor
    )
    .toScala
    .onComplete {
      case Success(state)     => println(state.value())
      case Failure(exception) => exception.printStackTrace()
    }
}
