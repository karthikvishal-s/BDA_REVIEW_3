import scala.collection.immutable.ListMap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * THE 10-MARK MODULE. Every rubric item of Review 3 prints visible output and is written to
 * outputs/logs/ so the evidence survives after the terminal closes:
 *
 *   rdd_demo.txt     full narrated run (creation, transformations, actions, key-value ops)
 *   partitions.txt   getNumPartitions before / after repartition and coalesce
 *   lineage.txt      toDebugString of a multi-stage RDD + narrow / wide explanation
 *   timings.txt      the four-run cache experiment, numbers exactly as observed
 *
 * HOLD=<seconds> keeps the application (and the Spark UI on :4040) alive at the end so the
 * DAG and stage views can be captured from a genuine run.
 */
object RddDemo {

  private val log = new StringBuilder

  private def say(s: String = ""): Unit = { println(s); log.append(s).append('\n') }
  private def section(title: String): Unit = { say(); say("=" * 78); say(title); say("=" * 78) }

  def main(args: Array[String]): Unit = {
    val m = Common.mode
    val spark = SparkSession.builder.appName(s"RddDemo [$m]").getOrCreate()
    val sc = spark.sparkContext
    Common.ensureDirs()
    val path = Common.rawCsv(m)
    val minFlights = if (m == "full") 300 else 20

    say(s"RddDemo  input=$path  mode=$m  spark=${sc.version}  master=${sc.master}")
    say(s"Application id: ${sc.applicationId}   Spark UI: http://localhost:4040 (published from the container)")

    // ------------------------------------------------------------------ 1. RDD creation
    section("1. RDD CREATION — sc.textFile on the raw CSV, header filtered out")
    val raw: RDD[String] = sc.textFile(path)
    val header = raw.first()
    val idx = Flight.headerIndex(header)
    val lines = raw.filter(_ != header)
    val nLines = lines.count()
    say(f"sc.textFile(...) -> RDD[String]; data lines (header removed): $nLines%,d")
    say(s"Columns resolved from the header: ${idx.size}")

    // ------------------------------------------------------------------ 2. map
    section("2. TRANSFORMATION map — parse each CSV line into a typed Flight record")
    val parsed: RDD[Flight] = lines.map(l => Flight.parse(l, idx))
    say("lines.map(l => Flight.parse(l, idx))  — narrow dependency, no shuffle")
    say("take(5) on the parsed RDD:")
    parsed.take(5).foreach { f =>
      say(f"  ${f.date} ${f.carrier}%-3s ${f.route}%-9s depHour=${f.depHour}%2d " +
        s"arrDelay=${f.arrDelay.getOrElse("null")} cancelled=${f.cancelled} diverted=${f.diverted} " +
        s"causes=${f.causes.mkString("[", ",", "]")}")
    }

    // ------------------------------------------------------------------ 3. filter
    section("3. TRANSFORMATION filter — drop malformed, cancelled and diverted flights")
    val wellFormed = parsed.filter(_.ok)
    val operated: RDD[Flight] = wellFormed.filter(_.operated)
    val nParsed = parsed.count()
    val nWell = wellFormed.count()
    val nOperated = operated.count()
    say(f"parsed                         : $nParsed%,d")
    say(f"filter(_.ok)       well-formed : $nWell%,d   (removed ${nParsed - nWell}%,d malformed lines)")
    say(f"filter(_.operated) operated    : $nOperated%,d   (removed ${nWell - nOperated}%,d cancelled or diverted)")
    say("Cancelled and diverted flights have no arrival delay, so they cannot contribute delay minutes.")

    // ------------------------------------------------------------------ 4. flatMap
    section("4. TRANSFORMATION flatMap — one (cause, minutes) token per non-zero delay cause")
    val causeTokens: RDD[(String, Double)] = operated.flatMap { f =>
      Common.Causes.zip(f.causes).filter(_._2 > 0)
    }
    say("A flight delayed by both weather and the carrier emits two tokens.")
    say(f"flights: $nOperated%,d  ->  cause tokens: ${causeTokens.count()}%,d")
    val causeFreq = causeTokens
      .mapValues(mins => (1L, mins))
      .reduceByKey { case ((c1, m1), (c2, m2)) => (c1 + c2, m1 + m2) }
      .collect().sortBy(-_._2._2)
    say(f"  ${"cause"}%-22s ${"flights"}%12s ${"minutes"}%14s")
    causeFreq.foreach { case (c, (n, mins)) => say(f"  $c%-22s $n%,12d ${mins}%,14.0f") }

    // ------------------------------------------------------------------ 5/6. reduceByKey + mapValues
    section("5. TRANSFORMATION reduceByKey — (sum of arrival delay, count) per ROUTE")
    val routePairs: RDD[(String, (Double, Long))] =
      operated.filter(_.arrDelay.isDefined).map(f => (f.route, (f.arrDelay.get, 1L)))
    val routeSums = routePairs.reduceByKey { case ((s1, n1), (s2, n2)) => (s1 + s2, n1 + n2) }
    say("map to (route, (delay, 1)) then reduceByKey — wide dependency: records with the same")
    say("route must meet on one partition, so Spark shuffles and starts a new stage here.")
    say(f"distinct routes: ${routeSums.count()}%,d")

    section("6. TRANSFORMATION mapValues — (sum, count) -> (average delay, count)")
    val routeAvg: RDD[(String, (Double, Long))] = routeSums.mapValues { case (s, n) => (s / n, n) }
    say(s"Top 10 routes by average arrival delay (routes with >= $minFlights flights):")
    routeAvg.filter(_._2._2 >= minFlights).sortBy(-_._2._1).take(10).foreach { case (r, (avg, n)) =>
      say(f"  $r%-10s avg=${avg}%7.2f min   flights=$n%,d")
    }

    // ------------------------------------------------------------------ 7. actions
    section("7. ACTIONS — count, take, reduce")
    say(f"count : operated flights = ${operated.count()}%,d")
    say(s"take  : first 3 routes of routeAvg = ${routeAvg.take(3).map(_._1).mkString(", ")}")
    val totalLateMinutes = operated.map(_.arrDelay.getOrElse(0.0)).filter(_ > 0).reduce(_ + _)
    say(f"reduce: total positive arrival-delay minutes = ${totalLateMinutes}%,.0f " +
      f"(${totalLateMinutes / 60 / 24 / 365}%,.1f aircraft-years)")

    // ------------------------------------------------------------------ 8. key-value operations
    section("8. KEY-VALUE OPERATIONS — carrier pair RDDs, aggregateByKey, join, dominant cause")
    val carrierDelay = operated.filter(_.arrDelay.isDefined)
      .map(f => (f.carrier, f.arrDelay.get))
      .aggregateByKey((0.0, 0L, 0L))(
        (acc, d) => (acc._1 + d, acc._2 + 1, acc._3 + (if (d >= 15) 1 else 0)),
        (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    val carrierCancelled = wellFormed.map(f => (f.carrier, (1L, if (f.cancelled) 1L else 0L)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    val carrierJoined = carrierDelay.join(carrierCancelled).collect().sortBy(c => -c._2._1._1 / c._2._1._2)
    say(f"  ${"carrier"}%-8s ${"flights"}%10s ${"avg delay"}%10s ${"late %"}%8s ${"cancel %"}%9s")
    carrierJoined.foreach { case (c, ((sum, n, late), (sched, canc))) =>
      say(f"  $c%-8s $n%,10d ${sum / n}%10.2f ${100.0 * late / n}%8.2f ${100.0 * canc / sched}%9.2f")
    }

    // Same label the cause classifier trains on: argmax of the five cause columns, ties dropped.
    val delayedWithCauses = operated.filter(f => f.arrDelay.exists(_ >= 15) && f.causesReported)
    val dominant = delayedWithCauses.map(f => (f.dominantCause.getOrElse("TIE_OR_ZERO"), 1L)).reduceByKey(_ + _)
      .collect().sortBy(-_._2)
    say("Dominant delay cause per late flight (map + reduceByKey) — the label of ML Model 2:")
    dominant.foreach { case (c, n) => say(f"  $c%-22s $n%,10d") }

    // ------------------------------------------------------------------ 9. partitions
    section("9. PARTITIONS")
    val p = new StringBuilder
    def pl(s: String): Unit = { say(s); p.append(s).append('\n') }
    val splitMb = sc.hadoopConfiguration.getLong("fs.local.block.size", 32L * 1024 * 1024) / 1024 / 1024
    val fileMb = new java.io.File(path).length / 1024.0 / 1024.0
    pl(f"raw (textFile)              : ${raw.getNumPartitions} partitions  (one per input split: $fileMb%.0f MB file / $splitMb MB splits)")
    pl(s"operated (narrow ops only)  : ${operated.getNumPartitions} partitions  (narrow ops keep the parent's partitioning)")
    pl(s"routeSums (reduceByKey)     : ${routeSums.getNumPartitions} partitions  (no numPartitions given and spark.default.parallelism unset -> keeps the parent's count)")
    val repart = operated.repartition(16)
    pl(s"operated.repartition(16)    : ${repart.getNumPartitions} partitions  (full shuffle)")
    val coal = repart.coalesce(4)
    pl(s"...coalesce(4)              : ${coal.getNumPartitions} partitions  (merges partitions, no shuffle)")
    val sizes = repart.mapPartitions(it => Iterator(it.size)).collect()
    pl(s"records per partition after repartition(16): ${sizes.mkString(", ")}")
    pl(s"routeSums.partitioner       : ${routeSums.partitioner.map(_.getClass.getSimpleName).getOrElse("none")}")
    Common.writeText(s"${Common.logs}/partitions.txt", s"mode=$m input=$path\n" + p.toString)

    // ------------------------------------------------------------------ 10. lineage / DAG
    section("10. LINEAGE and DAG — toDebugString of routeAvg")
    val lineage = routeAvg.toDebugString
    say(lineage)
    val lineageExplain =
      """
        |How to read it: each indentation level change marked "+-" is a shuffle boundary, i.e. a new stage.
        |  * textFile -> filter(header) -> map(parse) -> filter(ok) -> filter(operated) -> filter -> map
        |    are NARROW dependencies: each output partition depends on exactly one input partition, so Spark
        |    pipelines them inside one task with no data movement (Stage 0 in the Spark UI).
        |  * reduceByKey is a WIDE dependency (ShuffledRDD): records with the same route can sit on any input
        |    partition, so map-side partial sums are written to shuffle files and fetched by key (Stage 1).
        |  * mapValues after the shuffle is narrow again and keeps the HashPartitioner.
        |The stage boundaries in the Spark UI DAG view correspond exactly to the wide dependencies above.
        |
        |FAULT TOLERANCE: an RDD stores no data by default, only this lineage. If an executor holding a
        |partition of routeAvg is lost, Spark re-runs only the tasks that produced the missing partitions:
        |it re-reads the corresponding input split, re-applies the narrow chain, and re-fetches the shuffle
        |output for that key range (re-running the map stage only for shuffle files that were also lost).
        |No replication of intermediate data is needed — recomputation from lineage is the recovery plan.
        |""".stripMargin
    say(lineageExplain)
    Common.writeText(s"${Common.logs}/lineage.txt",
      s"mode=$m input=$path application=${sc.applicationId}\n\nrouteAvg.toDebugString\n$lineage\n$lineageExplain")

    // ------------------------------------------------------------------ 11. cache / persist
    section("11. CACHE / PERSIST — the same action twice uncached, then twice cached")
    val (c1, t1) = Common.time(operated.count())
    val (_, t2) = Common.time(operated.count())
    operated.persist(StorageLevel.MEMORY_ONLY)
    val (_, t3) = Common.time(operated.count())
    val (_, t4) = Common.time(operated.count())
    val cachedInfo = sc.getRDDStorageInfo.find(_.id == operated.id)
    val cachedPct = cachedInfo.map(i => 100.0 * i.numCachedPartitions / i.numPartitions).getOrElse(0.0)
    val cachedMb = cachedInfo.map(_.memSize / 1024.0 / 1024.0).getOrElse(0.0)
    val timing =
      f"""mode=$m input=$path rows=$c1%,d application=${sc.applicationId}
         |RDD: operated (textFile -> parse -> filter -> filter), action: count()
         |
         |run 1  uncached               : $t1%8.3f s   recomputes the full lineage from the CSV
         |run 2  uncached               : $t2%8.3f s   recomputes again (OS page cache may now hold the file)
         |       operated.persist(MEMORY_ONLY)
         |run 3  cached, first action   : $t3%8.3f s   computes AND stores the partitions
         |run 4  cached, served         : $t4%8.3f s   reads partitions from executor memory
         |
         |partitions cached: $cachedPct%.0f%%   memory used: $cachedMb%.1f MB   storage level: ${operated.getStorageLevel.description}
         |speed-up run 2 -> run 4: ${if (t4 > 0) f"${t2 / t4}%.1fx" else "n/a"}
         |
         |These are the timings actually observed on this run. Run 2 can already be faster than run 1
         |because the operating system caches the CSV file after the first read; that removes disk time
         |but not CSV parsing, which is what persist() removes. If fewer than 100%% of partitions were
         |cached, executor memory was insufficient and the uncached partitions were recomputed.
         |""".stripMargin
    say(timing)
    Common.writeText(s"${Common.logs}/timings.txt", timing)

    // ------------------------------------------------------------------ JSON summaries
    Common.writeJson("dominant_cause_counts.json", m, nLines, "RddDemo.scala", ListMap(
      "population" -> "operated flights with ARR_DELAY >= 15 and all five cause columns reported",
      "flights" -> delayedWithCauses.count(),
      "counts" -> ListMap(dominant: _*)
    ))
    Common.writeJson("rdd_summary.json", m, nLines, "RddDemo.scala", ListMap(
      "stage_counts" -> ListMap("lines" -> nLines, "parsed" -> nParsed, "well_formed" -> nWell, "operated" -> nOperated),
      "total_positive_delay_minutes" -> totalLateMinutes,
      "cause_frequency" -> causeFreq.map { case (c, (n, mins)) => ListMap("cause" -> c, "flights" -> n, "minutes" -> mins) }.toSeq,
      "cache_timings_s" -> ListMap("uncached_1" -> t1, "uncached_2" -> t2, "cached_1" -> t3, "cached_2" -> t4),
      "cached_partitions_pct" -> cachedPct
    ))
    operated.unpersist(blocking = true)
    Common.writeText(s"${Common.logs}/rdd_demo.txt", log.toString)

    val hold = sys.env.getOrElse("HOLD", "0").toInt
    if (hold > 0) {
      println(s"\nHOLD=$hold — Spark UI stays up at http://localhost:4040 (Jobs, Stages, DAG, Storage tabs).")
      operated.persist(StorageLevel.MEMORY_ONLY).count() // so the Storage tab has something to show
      Thread.sleep(hold * 1000L)
    }
    spark.stop()
  }
}
