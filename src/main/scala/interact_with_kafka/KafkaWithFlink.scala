package interact_with_kafka

import java.util.Properties

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerConfig

object KafkaWithFlink extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val props = new Properties()

  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")

  val kafkaSource = env.addSource(
    new FlinkKafkaConsumer[String](
      "kafka-topic-here",
      new SimpleStringSchema(),
      props
    )
  )

  kafkaSource
    .flatMap(new FlatMapFunction[String, (String, Int)] {
      override def flatMap(
          value: String,
          out: Collector[(String, Int)]
      ): Unit = {
        val words = value.split(" ")

        for (word <- words) {
          out.collect((word, 1))
        }
      }
    })
    .keyBy(0)
    .sum(1)
    .writeAsText("tmp/kafka_demo")

  env.execute("Kafka demo")
}
