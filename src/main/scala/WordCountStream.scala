import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object WordCountStream extends App {
  // set up the execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // make parameters available in web interfaces
  env.getConfig.setGlobalJobParameters(ParameterTool.fromArgs(args))

  // get input data
  val text = env.socketTextStream("localhost", 9999)

  val counts = text
    .flatMap { _.toLowerCase.split("\\W+") }
    .map { (_, 1) }
    .keyBy(0)
    .sum(1)

  // execute and print result
  counts.print()

  env.execute("Streaming word count")
}
