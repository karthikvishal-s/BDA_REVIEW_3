import java.io.{File, PrintWriter}
import java.time.Instant

import scala.collection.immutable.ListMap
import scala.util.Try

/** Paths, column contract and small helpers shared by Ingest, RddDemo and RouteAnalytics. */
object Common {

  val Root: String = sys.env.getOrElse("PROJECT_ROOT", "/work")

  /** INPUT=full|sample|synthetic selects which CSV every stage reads. */
  def mode: String = sys.env.getOrElse("INPUT", "sample")

  def rawCsv(m: String): String = m match {
    case "full"      => s"$Root/data/raw/flight_data_cleaned_1year.csv"
    case "sample"    => s"$Root/data/samples/sample_100k.csv"
    case "synthetic" => s"$Root/data/samples/synthetic_smoke.csv"
    case other       => sys.error(s"Unknown INPUT '$other' (expected full, sample or synthetic)")
  }

  def processed(m: String): String = s"$Root/data/processed/$m/flights.parquet"
  def logs: String   = s"$Root/outputs/logs"
  def scalaOut: String = s"$Root/outputs/scala"

  val Causes: Seq[String] =
    Seq("CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY")

  /** Columns every later stage depends on. Ingest refuses to run if any is missing. */
  val Required: Seq[String] = Seq(
    "FL_DATE", "OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST",
    "CRS_DEP_TIME", "CRS_ARR_TIME", "DEP_TIME", "DEP_DELAY", "ARR_DELAY",
    "CANCELLED", "DIVERTED", "DISTANCE"
  ) ++ Causes

  /** Used when present; derived (ROUTE, MONTH, DAY_OF_WEEK) or skipped otherwise. */
  val Optional: Seq[String] = Seq(
    "ROUTE", "MONTH", "DAY_OF_WEEK", "DEP_TIME_CATEGORY", "DISTANCE_CATEGORY", "DELAY_CATEGORY",
    "TAXI_OUT", "TAXI_IN", "AIR_TIME", "ARR_TIME", "ARR_DEL15", "DEP_DEL15", "ARR_DELAY_NEW",
    "ACTUAL_ELAPSED_TIME", "CRS_ELAPSED_TIME"
  )

  // ---------------------------------------------------------------- parsing helpers

  /** Splits one CSV line, honouring double quotes (city names such as "New York, NY"). */
  def splitCsv(line: String): Array[String] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[String]
    val sb = new StringBuilder
    var inQuotes = false
    var i = 0
    while (i < line.length) {
      val c = line.charAt(i)
      if (c == '"') {
        if (inQuotes && i + 1 < line.length && line.charAt(i + 1) == '"') { sb.append('"'); i += 1 }
        else inQuotes = !inQuotes
      } else if (c == ',' && !inQuotes) { out += sb.toString; sb.clear() }
      else sb.append(c)
      i += 1
    }
    out += sb.toString
    out.toArray
  }

  def toDouble(s: String): Option[Double] = {
    val t = s.trim
    if (t.isEmpty || t.equalsIgnoreCase("NA") || t.equalsIgnoreCase("null")) None
    else Try(t.toDouble).toOption.filter(d => !d.isNaN)
  }

  def toFlag(s: String): Boolean = {
    val t = s.trim.toLowerCase
    t == "true" || toDouble(t).exists(_ >= 0.5)
  }

  /** Hour 0-23 from an hhmm value written as 755, 0755, 755.0 or 07:55. */
  def hourOf(s: String): Option[Int] = {
    val t = s.trim
    if (t.contains(":")) Try(t.split(":")(0).toInt % 24).toOption
    else toDouble(t).map(v => (v.toInt / 100) % 24)
  }

  /** ISO yyyy-MM-dd, or the BTS export style M/d/yyyy [h:mm:ss AM]. Returns ISO date. */
  def isoDate(s: String): Option[String] = {
    val t = s.trim.split(" ")(0)
    if (t.matches("""\d{4}-\d{2}-\d{2}""")) Some(t)
    else t.split("/") match {
      case Array(m, d, y) if y.length == 4 => Try(f"${y.toInt}%04d-${m.toInt}%02d-${d.toInt}%02d").toOption
      case _ => None
    }
  }

  // ---------------------------------------------------------------- output helpers

  def ensureDirs(): Unit = Seq(logs, scalaOut).foreach(d => new File(d).mkdirs())

  def writeText(path: String, text: String): Unit = {
    new File(path).getParentFile.mkdirs()
    val w = new PrintWriter(path, "UTF-8")
    try w.write(text) finally w.close()
  }

  /** Provenance stamp written into every JSON file (doc §3.1). */
  def meta(m: String, rows: Long, producer: String): ListMap[String, Any] = ListMap(
    "input_path" -> rawCsv(m).stripPrefix(Root + "/"),
    "mode"       -> m,
    "row_count"  -> rows,
    "generated_at" -> Instant.now().toString,
    "producer"   -> producer
  )

  def writeJson(name: String, m: String, rows: Long, producer: String, body: ListMap[String, Any]): String = {
    val path = s"$scalaOut/$name"
    writeText(path, Json(ListMap("_meta" -> meta(m, rows, producer)) ++ body) + "\n")
    path
  }

  def time[T](block: => T): (T, Double) = {
    val t0 = System.nanoTime()
    val r = block
    (r, (System.nanoTime() - t0) / 1e9)
  }
}

/** Minimal JSON writer — enough for the summary files, no extra dependency. */
object Json {
  private def esc(s: String): String = s.flatMap {
    case '"'  => "\\\""
    case '\\' => "\\\\"
    case '\n' => "\\n"
    case '\r' => "\\r"
    case '\t' => "\\t"
    case c if c < ' ' => f"\\u${c.toInt}%04x"
    case c => c.toString
  }

  def apply(v: Any): String = v match {
    case null | None => "null"
    case Some(x)     => apply(x)
    case s: String   => "\"" + esc(s) + "\""
    case d: Double   => if (d.isNaN || d.isInfinite) "null" else (math.round(d * 10000) / 10000.0).toString
    case f: Float    => apply(f.toDouble)
    case b: Boolean  => b.toString
    case n: Int      => n.toString
    case n: Long     => n.toString
    case m: scala.collection.Map[_, _] =>
      m.map { case (k, x) => apply(k.toString) + ":" + apply(x) }.mkString("{", ",", "}")
    case xs: Iterable[_] => xs.map(apply).mkString("[", ",", "]")
    case a: Array[_]     => apply(a.toSeq)
    case p: Product      => apply(p.productIterator.toSeq)
    case other           => apply(other.toString)
  }
}

/** One flight leg. `ok` is false when the line could not be parsed into a usable record. */
case class Flight(
    date: String,
    month: Int,
    dayOfWeek: Int,
    carrier: String,
    origin: String,
    dest: String,
    route: String,
    depHour: Int,
    depDelay: Option[Double],
    arrDelay: Option[Double],
    cancelled: Boolean,
    diverted: Boolean,
    distance: Double,
    causes: Array[Double],
    causesReported: Boolean,
    ok: Boolean
) {
  def operated: Boolean = !cancelled && !diverted

  /** Name of the single largest cause, or None for no-cause rows and exact ties. */
  def dominantCause: Option[String] = {
    if (!causesReported) None
    else {
      val mx = causes.max
      if (mx <= 0 || causes.count(_ == mx) > 1) None
      else Some(Common.Causes(causes.indexOf(mx)))
    }
  }
}

object Flight {
  private val Bad = Flight("", 0, 0, "", "", "", "", -1, None, None, false, false, 0.0,
    Array.fill(5)(0.0), false, ok = false)

  /** Parses a raw CSV line with a header-derived column index (column order never assumed). */
  def parse(line: String, idx: Map[String, Int]): Flight = Try {
    val f = Common.splitCsv(line)
    def get(c: String): String = idx.get(c).filter(_ < f.length).map(i => f(i).trim).getOrElse("")

    val date = Common.isoDate(get("FL_DATE")).get
    val origin = get("ORIGIN"); val dest = get("DEST"); val carrier = get("OP_UNIQUE_CARRIER")
    require(origin.nonEmpty && dest.nonEmpty && carrier.nonEmpty)

    val ld = java.time.LocalDate.parse(date)
    val month = Common.toDouble(get("MONTH")).map(_.toInt).getOrElse(ld.getMonthValue)
    val dow = Common.toDouble(get("DAY_OF_WEEK")).map(_.toInt).getOrElse(ld.getDayOfWeek.getValue)
    val route = Some(get("ROUTE")).filter(_.nonEmpty).getOrElse(s"${origin}_$dest")
    val causeVals = Common.Causes.map(c => Common.toDouble(get(c)))

    Flight(
      date, month, dow, carrier, origin, dest, route,
      depHour = Common.hourOf(get("CRS_DEP_TIME")).getOrElse(-1),
      depDelay = Common.toDouble(get("DEP_DELAY")),
      arrDelay = Common.toDouble(get("ARR_DELAY")),
      cancelled = Common.toFlag(get("CANCELLED")),
      diverted = Common.toFlag(get("DIVERTED")),
      distance = Common.toDouble(get("DISTANCE")).getOrElse(0.0),
      causes = causeVals.map(_.getOrElse(0.0)).toArray,
      causesReported = causeVals.forall(_.isDefined),
      ok = true
    )
  }.getOrElse(Bad)

  def headerIndex(header: String): Map[String, Int] =
    Common.splitCsv(header).map(_.trim.stripPrefix("﻿").toUpperCase).zipWithIndex.toMap
}
