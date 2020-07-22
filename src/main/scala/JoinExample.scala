import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object JoinExample extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val params = ParameterTool.fromArgs(args)

  env.getConfig.setGlobalJobParameters(params)

  val remapInput: String => (Int, String) = { row =>
    val words: Array[String] = row.split(",")

    (words(0).toInt, words(1))
  }

  val personSet = env
    .readTextFile(params.get("input1"))
    .map(remapInput)

  val locationSet = env
    .readTextFile(params.get("input2"))
    .map(remapInput)

  val joined: DataSet[(Int, String, String)] = personSet
    .join(locationSet)
    .where(0)
    .equalTo(0) { (l, r) =>
      (l._1, l._2, r._2)
    }

  joined.writeAsCsv(params.get("output"), "\n", " ")

  env.execute("JoinExample")
}
