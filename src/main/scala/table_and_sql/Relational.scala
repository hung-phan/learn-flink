package table_and_sql

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, FieldExpression}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource

object Relational extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val bsSettings = EnvironmentSettings
    .newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()
  val tableEnv = StreamTableEnvironment.create(env, bsSettings)

  /**
    * INPUT TABLE
    */

  // from CSV
  val tableSrc = CsvTableSource
    .builder()
    .path("")
    .ignoreFirstLine()
    .fieldDelimiter(",")
    .field("column1", DataTypes.INT())
    .field("column2", DataTypes.STRING())
    .field("column3", DataTypes.DOUBLE())
    .build()

  // deprecated: Refrain yourself from using this
  tableEnv.registerTableSource("CsvTable", tableSrc)

  // from data stream
  val data = env
    .readTextFile("/path/to/file")
    .map { ("", "", _) }

  tableEnv.registerDataStream(
    "StreamTable",
    data,
    $"column1",
    $"column2",
    $"column3"
  )

  // create table from another table
  val projectTable = tableEnv
    .scan("StreamTable")
    .select($"column1", $"column3")

  /**
    * OUTPUT TABLE
    */

  // write to csv
  val csvSink = new CsvTableSink("/path/to/file", ",")
  val fieldNames: Array[String] = Array("column1", "column2", "column3")
  val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.LONG)

  tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)

  // table to data stream
  val newStream = tableEnv.toAppendStream[(String, String)](projectTable)
}
