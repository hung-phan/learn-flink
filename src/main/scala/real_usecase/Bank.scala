package real_usecase

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object Bank extends App {
  case class AlarmedCustomer(id: String, account: String)

  object AlarmedCustomer {
    def apply(data: String): AlarmedCustomer = {
      val words = data.split(",")

      AlarmedCustomer(words(0), words(1))
    }
  }

  case class LostCard(
      id: String,
      timestamp: String,
      name: String,
      status: String
  )

  object LostCard {
    def apply(data: String): LostCard = {
      val words = data.split(",")

      LostCard(words(0), words(1), words(2), words(3))
    }
  }

  val alarmedCustStateDescriptor =
    new MapStateDescriptor[String, AlarmedCustomer](
      "alarmed_customers",
      BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[AlarmedCustomer] {})
    )
  val lostCardStateDescriptor = new MapStateDescriptor[String, LostCard](
    "lost_cards",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(new TypeHint[LostCard] {})
  )

  class LostCardCheck
      extends KeyedBroadcastProcessFunction[
        String,
        (String, String),
        LostCard,
        (String, String)
      ] {
    override def processElement(
        value: (String, String),
        ctx: KeyedBroadcastProcessFunction[
          String,
          (String, String),
          LostCard,
          (String, String)
        ]#ReadOnlyContext,
        out: Collector[(String, String)]
    ): Unit = {
      for (
        cardEntry <-
          ctx
            .getBroadcastState(lostCardStateDescriptor)
            .immutableEntries
            .asScala
      ) {
        val lostCardId = cardEntry.getKey
        // get card_id of current transaction
        val cId = value._2.split(",")(5)

        if (cId == lostCardId)
          out.collect(
            ("__ALARM__", s"Transaction: $value issued via LOST card")
          )
      }
    }

    override def processBroadcastElement(
        value: LostCard,
        ctx: KeyedBroadcastProcessFunction[
          String,
          (String, String),
          LostCard,
          (String, String)
        ]#Context,
        out: Collector[(String, String)]
    ): Unit = {
      ctx.getBroadcastState(lostCardStateDescriptor).put(value.id, value)
    }
  }

  class AlarmedCustCheck
      extends KeyedBroadcastProcessFunction[
        String,
        (String, String),
        AlarmedCustomer,
        (String, String)
      ] {

    override def processElement(
        value: (String, String),
        ctx: KeyedBroadcastProcessFunction[
          String,
          (String, String),
          AlarmedCustomer,
          (String, String)
        ]#ReadOnlyContext,
        out: Collector[(String, String)]
    ): Unit = {
      for (
        custEntry <-
          ctx
            .getBroadcastState(alarmedCustStateDescriptor)
            .immutableEntries
            .asScala
      ) {
        val alarmedCustId = custEntry.getKey
        // get customer_id of current transaction
        val tId = value._2.split(",")(3)

        if (tId == alarmedCustId)
          out.collect(
            ("____ALARM___", s"Transaction: $value is by an ALARMED customer")
          )
      }
    }

    override def processBroadcastElement(
        value: AlarmedCustomer,
        ctx: KeyedBroadcastProcessFunction[
          String,
          (String, String),
          AlarmedCustomer,
          (String, String)
        ]#Context,
        out: Collector[(String, String)]
    ): Unit = {
      ctx.getBroadcastState(alarmedCustStateDescriptor).put(value.id, value)
    }
  }

  class Citychange
      extends ProcessWindowFunction[
        (String, String),
        (String, String),
        String,
        TimeWindow
      ] {
    override def process(
        key: String,
        context: Context,
        elements: Iterable[(String, String)],
        out: Collector[(String, String)]
    ): Unit = {
      var lastCity = ""
      var changeCount = 0

      for (element <- elements) {
        val city = element._2.split(",")(2).toLowerCase

        if (lastCity.isEmpty) {
          lastCity = city
        } else if (!(city == lastCity)) {
          lastCity = city
          changeCount += 1
        }

        if (changeCount >= 2)
          out.collect(
            ("__ALARM__", s"${element}marked for FREQUENT city changes")
          )
      }
    }
  }

  class FilterAndMapMoreThan10
      extends FlatMapFunction[(String, String, Int), (String, String)] {
    override def flatMap(
        value: (String, String, Int),
        out: Collector[(String, String)]
    ): Unit = {
      if (value._3 > 10) {
        out.collect(("__ALARM__", s"$value marked for >10 TXNs"))
      }
    }
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val alarmedCustomers = env
    .readTextFile(
      getClass.getResource("/real_usecase/alarmed_cust.txt").getPath
    )
    .map { AlarmedCustomer(_) }

  val lostCards = env
    .readTextFile(
      getClass.getResource("/real_usecase/lost_cards.txt").getPath
    )
    .map { LostCard(_) }

  // broadcast alarmed customer data
  val alarmedCustBroadcast =
    alarmedCustomers.broadcast(alarmedCustStateDescriptor)
  // broadcast lost card data
  val lostCardBroadcast =
    lostCards.broadcast(lostCardStateDescriptor)

  // transaction data keyed by customer_id
  val data = env
    .socketTextStream("localhost", 9090)
    .map { value =>
      val words = value.split(",")

      (words(3), value)
    }

  data.print()

  // (1) Check against alarmed customers
  val alarmedCustTransactions = data
    .keyBy(0)
    .connect(alarmedCustBroadcast)
    .process(new AlarmedCustCheck)

  // (2) Check against lost cards
  val lostCardTransactions = data
    .keyBy(0)
    .connect(lostCardBroadcast)
    .process(new LostCardCheck)

  val excessiveTransactions = data
    .map { value => (value._1, value._2, 1) }
    .keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .sum(2)
    .flatMap(new FilterAndMapMoreThan10)

  val freqCityChangeTransactions = data
    .keyBy { _._1 }
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .process(new Citychange)

  val AllFlaggedTxn = alarmedCustTransactions.union(
    lostCardTransactions,
    excessiveTransactions,
    freqCityChangeTransactions
  )

  AllFlaggedTxn.writeAsText("tmp/flagged_transaction")

  env.execute("Streaming Bank")
}
