package graph_api

import org.apache.flink.api.scala._
import org.apache.flink.graph.Edge
import org.apache.flink.graph.library.SingleSourceShortestPaths
import org.apache.flink.graph.scala.Graph
import org.apache.flink.types.NullValue

object GraphApiExample extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment

  /**
    * format user,friend
    */
  val friends = env
    .readTextFile("/path/to/file")
    .map { line =>
      val words = line.split("\\s+")

      (words(0), words(1))
    }

  // prepare normal dataset to edges for graph
  val edges = friends
    .map { value =>
      new Edge[String, NullValue](value._1, value._2, NullValue.getInstance())
    }

  val friendsGraph = Graph.fromDataSet(edges, env)
  val weightedFriendsGraph = friendsGraph.mapEdges { _ => Double.box(1.0 }

  // Get all friends of friends of friends of ...
  val userName = "hung"

  val result = weightedFriendsGraph.run(
    new SingleSourceShortestPaths[String, NullValue](userName, 10)
  )

  // Get only friends of friends for hung
  val forUserHung = result.filter { value =>
    value.f1.compareTo(2.0) == 0
  }

  forUserHung.writeAsText("/path/to/file")

  env.execute("Graph processing")
}
