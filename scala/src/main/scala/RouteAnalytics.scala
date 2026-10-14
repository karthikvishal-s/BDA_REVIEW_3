import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Route, carrier, cause, temporal, airport and cancellation analytics as pair-RDD jobs over the
 * Parquet file written by Ingest. Output goes to JSON files in outputs/scala/ and feeds viz/make_plots.py,
 * so the Scala and Python stages report the same numbers.
 */
object RouteAnalytics {

  /** (flights, sum delay, late >= 15) accumulator used by every grouping below. */
  type Acc = (Long, Double, Long)
  private val zero: Acc = (0L, 0.0, 0L)
  private def add(a: Acc, d: Double): Acc = (a._1 + 1, a._2 + d, a._3 + (if (d >= 15) 1 else 0))
  private def merge(a: Acc, b: Acc): Acc = (a._1 + b._1, a._2 + b._2, a._3 + b._3)

  private def stats[K: ClassTag](rdd: RDD[(K, Double)]): RDD[(K, Acc)] = rdd.aggregateByKey(zero)(add, merge)

  private def row(key: String, k: Any, a: Acc): ListMap[String, Any] = ListMap(
    key -> k, "flights" -> a._1, "avg_arr_delay" -> a._2 / a._1, "delay_rate" -> a._3.toDouble / a._1)

  def main(args: Array[String]): Unit = {
    val m = Common.mode
    val spark = SparkSession.builder.appName(s"RouteAnalytics [$m]").getOrCreate()
    Common.ensureDirs()
    val minRoute = if (m == "full") 300 else 20
    val minAirport = if (m == "full") 1000 else 50

    val cols = Seq("FL_DATE", "MONTH", "DAY_OF_WEEK", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "ROUTE",
      "DEP_HOUR", "DEP_DELAY", "ARR_DELAY", "CANCELLED", "DIVERTED", "DISTANCE") ++ Common.Causes
    val df = spark.read.parquet(Common.processed(m)).select(cols.head, cols.tail: _*)

    def d(r: Row, i: Int): Option[Double] = if (r.isNullAt(i)) None else Some(r.getDouble(i))
    val flights: RDD[Flight] = df.rdd.map { r =>
      val causes = (13 until 18).map(i => d(r, i))
      Flight(
        r.getDate(0).toString, r.getInt(1), r.getInt(2), r.getString(3), r.getString(4), r.getString(5),
        r.getString(6), if (r.isNullAt(7)) -1 else r.getInt(7), d(r, 8), d(r, 9),
        r.getInt(10) == 1, r.getInt(11) == 1, d(r, 12).getOrElse(0.0),
        causes.map(_.getOrElse(0.0)).toArray, causes.forall(_.isDefined), ok = true)
    }.cache()

    val total = flights.count()
    val operated = flights.filter(f => f.operated && f.arrDelay.isDefined)
    val nOperated = operated.count()
    def write(name: String, body: ListMap[String, Any]): Unit =
      println("wrote " + Common.writeJson(name, m, total, "RouteAnalytics.scala", body))

    // ---------------------------------------------------------------- routes
    val routeStats = stats(operated.map(f => (f.route, f.arrDelay.get)))
      .filter(_._2._1 >= minRoute).collect()
    val routeDist = operated.map(f => (f.route, f.distance)).reduceByKey((a, b) => math.max(a, b)).collectAsMap()
    def routeRows(sorted: Seq[(String, Acc)]) = sorted.take(50).map { case (k, a) =>
      row("route", k, a) ++ ListMap("distance" -> routeDist.getOrElse(k, 0.0)) }
    write("route_stats.json", ListMap(
      "min_flights" -> minRoute,
      "routes_considered" -> routeStats.length,
      "by_avg_delay" -> routeRows(routeStats.sortBy(r => -r._2._2 / r._2._1)),
      "by_delay_rate" -> routeRows(routeStats.sortBy(r => -r._2._3.toDouble / r._2._1)),
      "busiest" -> routeRows(routeStats.sortBy(-_._2._1))
    ))

    // ---------------------------------------------------------------- carriers (join of two pair RDDs)
    val carrierDelay = stats(operated.map(f => (f.carrier, f.arrDelay.get)))
    val carrierOps = flights.map(f => (f.carrier, (1L, if (f.cancelled) 1L else 0L, if (f.diverted) 1L else 0L)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    val carriers = carrierDelay.join(carrierOps).collect().sortBy(-_._2._1._1).map {
      case (c, (a, (sched, canc, div))) =>
        row("carrier", c, a) ++ ListMap("scheduled" -> sched, "cancelled" -> canc, "diverted" -> div,
          "cancel_rate" -> canc.toDouble / sched)
    }
    write("carrier_stats.json", ListMap("carriers" -> carriers.toSeq))

    // ---------------------------------------------------------------- causes
    val delayed = operated.filter(f => f.arrDelay.get >= 15 && f.causesReported)
    val causeTotals = delayed.flatMap(f => Common.Causes.zip(f.causes).filter(_._2 > 0))
      .aggregateByKey((0L, 0.0))((a, v) => (a._1 + 1, a._2 + v), (a, b) => (a._1 + b._1, a._2 + b._2))
      .collectAsMap()
    val allMinutes = causeTotals.values.map(_._2).sum
    val dominant = delayed.flatMap(_.dominantCause).map((_, 1L)).reduceByKey(_ + _).collectAsMap()
    val causeMonth = delayed.flatMap(f => Common.Causes.zip(f.causes).map { case (c, v) => ((f.month, c), v) })
      .reduceByKey(_ + _).collect()
    write("causes.json", ListMap(
      "population" -> "operated flights with ARR_DELAY >= 15 and cause columns reported",
      "delayed_flights" -> delayed.count(),
      "causes" -> Common.Causes.map { c =>
        val (n, mins) = causeTotals.getOrElse(c, (0L, 0.0))
        ListMap("cause" -> c, "flights_with_cause" -> n, "minutes" -> mins,
          "minutes_share" -> (if (allMinutes > 0) mins / allMinutes else 0.0),
          "dominant_count" -> dominant.getOrElse(c, 0L))
      },
      "minutes_by_month" -> causeMonth.groupBy(_._1._1).toSeq.sortBy(_._1).map { case (mo, xs) =>
        ListMap("month" -> mo) ++ ListMap(xs.map { case ((_, c), v) => c -> v }.sortBy(_._1): _*)
      }
    ))

    // ---------------------------------------------------------------- temporal
    def byKey(f: Flight => Int) = stats(operated.map(x => (f(x), x.arrDelay.get))).collect().sortBy(_._1)
    write("temporal.json", ListMap(
      "by_month" -> byKey(_.month).map { case (k, a) => row("month", k, a) }.toSeq,
      "by_day_of_week" -> byKey(_.dayOfWeek).map { case (k, a) => row("day_of_week", k, a) }.toSeq,
      "by_dep_hour" -> byKey(_.depHour).filter(_._1 >= 0).map { case (k, a) => row("dep_hour", k, a) }.toSeq
    ))

    // ---------------------------------------------------------------- airports (on-time performance)
    val airports = stats(operated.map(f => (f.origin, f.arrDelay.get))).filter(_._2._1 >= minAirport)
      .collect().sortBy(-_._2._1).take(40).map { case (k, a) =>
        row("airport", k, a) ++ ListMap("on_time_pct" -> 100.0 * (a._1 - a._3) / a._1) }
    write("airport_otp.json", ListMap("min_flights" -> minAirport, "airports" -> airports.toSeq))

    // ---------------------------------------------------------------- cancellations and diversions
    val byMonth = flights.map(f => (f.month, (1L, if (f.cancelled) 1L else 0L, if (f.diverted) 1L else 0L)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)).collect().sortBy(_._1)
    write("cancellations.json", ListMap(
      "scheduled" -> total,
      "operated_with_arrival" -> nOperated,
      "by_month" -> byMonth.map { case (mo, (s, c, dv)) =>
        ListMap("month" -> mo, "scheduled" -> s, "cancelled" -> c, "diverted" -> dv, "cancel_rate" -> c.toDouble / s) }.toSeq,
      "by_carrier" -> carriers.map(c => ListMap("carrier" -> c("carrier"), "scheduled" -> c("scheduled"),
        "cancelled" -> c("cancelled"), "diverted" -> c("diverted"), "cancel_rate" -> c("cancel_rate"))).toSeq
    ))

    spark.stop()
  }
}
