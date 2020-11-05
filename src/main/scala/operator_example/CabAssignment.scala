package operator_example

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object CabAssignment extends App {
  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  val text =
    env.readTextFile(getClass().getResource("cab+flink.txt").getPath, "UTF-8")

  val data = text
    .map { row =>
      val Array(
        cabID,
        cabNumberPlate,
        cabType,
        cabDriverName,
        ongoingTrip,
        pickupLocation,
        destination,
        passengerCount
      ) = row.split(",")

      (
        cabID,
        cabNumberPlate,
        cabType,
        cabDriverName,
        ongoingTrip,
        pickupLocation,
        destination,
        passengerCount match {
          case "'null'" => 0
          case str      => str.toInt
        }
      )
    }

  val popularDestination = data
    .map { input =>
      (input._7, input._8)
    }
    .filter(_._1 != "'null'")
    .groupBy(0)
    .sum(1)
    .max(1)

  popularDestination.print()

  val averageNumberOfPassengers = data
    .map { input =>
      (input._6, input._8, 1)
    }
    .filter(_._1 != "'null'")
    .groupBy(0)
    .reduce { (curr, prev) =>
      (curr._1, curr._2 + prev._2, curr._3 + prev._3)
    }
    .map { input =>
      (input._1, input._2 * 1.0 / input._3)
    }

  averageNumberOfPassengers.print()

  val averageNumberOfTrips = data
    .map { input =>
      (input._1, input._4, input._8, 1)
    }
    .groupBy(0)
    .reduce { (curr, prev) =>
      (curr._1, curr._2, curr._3 + prev._3, curr._4 + prev._4)
    }
    .map { input =>
      (input._1, input._2, input._3 * 1.0 / input._4)
    }

  averageNumberOfTrips.print()
}
