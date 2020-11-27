package table_and_sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.sources.CsvTableSource

object TableApiSample extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv = BatchTableEnvironment.create(env)

  val tableSrc = CsvTableSource
    .builder()
    .path("/path/to/file")
    .field("date", DataTypes.STRING())
    .field("month", DataTypes.STRING())
    .field("category", DataTypes.STRING())
    .field("product", DataTypes.STRING())
    .field("profit", DataTypes.INT())
    .build()

  tableEnv.registerTableSource("CatalogTable", tableSrc)

  /**
    * query with table API
    */
  val category5Profit = tableEnv
    .from("CatalogTable")
    .filter($"catalog" === "'Category5'")
    .groupBy($"month")
    .select($"month", $"profit".sum as "sum")
    .orderBy($"sum")

  val category5ProfitSet = tableEnv.toDataSet[(String, Int)](category5Profit)
  category5ProfitSet.writeAsText("/path/to/file")

  // sql
  val newCategory5Profit = tableEnv.sqlQuery(
    "SELECT `month`, SUM(profit) as sum1 FROM CatalogTable WHERE category = 'Category5' GROUP BY `month` ORDER BY sum1"
  )

  val newCategory5ProfitSet =
    tableEnv.toDataSet[(String, Int)](newCategory5Profit)
  newCategory5ProfitSet.writeAsText("/path/to/file")

  env.execute("TableApiExample")
}
