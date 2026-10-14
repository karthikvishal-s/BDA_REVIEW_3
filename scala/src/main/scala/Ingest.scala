import scala.collection.immutable.ListMap

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}

/**
 * CSV -> Parquet (partitioned by MONTH) with validation counts.
 *
 *   spark-submit --class Ingest review3-flight-spark.jar [--check-only]
 *
 * --check-only compares the CSV header with the expected column contract and stops.
 */
object Ingest {

  private val NumericCols = Seq(
    "CRS_DEP_TIME", "DEP_TIME", "CRS_ARR_TIME", "ARR_TIME", "DEP_DELAY", "ARR_DELAY", "ARR_DELAY_NEW",
    "DISTANCE", "TAXI_OUT", "TAXI_IN", "AIR_TIME", "ACTUAL_ELAPSED_TIME", "CRS_ELAPSED_TIME",
    "ARR_DEL15", "DEP_DEL15"
  ) ++ Common.Causes

  def main(args: Array[String]): Unit = {
    val m = Common.mode
    val spark = SparkSession.builder
      .appName(s"Ingest [$m]")
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .getOrCreate()
    Common.ensureDirs()

    val path = Common.rawCsv(m)
    require(new java.io.File(path).exists, s"Input not found: $path")

    val log = new StringBuilder
    def say(s: String = ""): Unit = { println(s); log.append(s).append('\n') }

    // ---------------------------------------------------------------- schema check
    val header = spark.sparkContext.textFile(path).first()
    val cols = Flight.headerIndex(header).toSeq.sortBy(_._2).map(_._1)
    val missing = Common.Required.filterNot(cols.contains)
    val optionalMissing = Common.Optional.filterNot(cols.contains)
    val extra = cols.filterNot(c => Common.Required.contains(c) || Common.Optional.contains(c))

    say(s"=== Schema check: $path")
    say(s"Columns in file          : ${cols.size}")
    say(s"Required present         : ${Common.Required.size - missing.size}/${Common.Required.size}")
    say(s"Required MISSING         : ${if (missing.isEmpty) "none" else missing.mkString(", ")}")
    say(s"Optional missing (derived or skipped): ${if (optionalMissing.isEmpty) "none" else optionalMissing.mkString(", ")}")
    say(s"Other columns (passed through as text): ${extra.mkString(", ")}")
    say(s"Header: ${cols.mkString(", ")}")

    if (args.contains("--check-only")) {
      say("\nFirst three data lines:")
      spark.sparkContext.textFile(path).take(4).drop(1).foreach(l => say("  " + l))
      Common.writeText(s"${Common.logs}/schema_check.txt", log.toString)
      spark.stop()
      if (missing.nonEmpty) sys.exit(1)
      return
    }
    if (missing.nonEmpty) {
      Common.writeText(s"${Common.logs}/schema_check.txt", log.toString)
      spark.stop()
      sys.error(s"Required columns missing: ${missing.mkString(", ")}")
    }

    // ---------------------------------------------------------------- read as text, cast by name
    val raw = spark.read
      .option("header", "true")
      .option("escape", "\"")
      .csv(path)
    val upper = raw.toDF(raw.columns.map(_.trim.stripPrefix("﻿").toUpperCase): _*)
    val present = upper.columns.toSet

    def str(c: String): Column = F.trim(F.col(c))
    def num(c: String): Column = str(c).cast("double")
    def hour(c: String): Column =
      F.when(str(c).contains(":"), F.split(str(c), ":").getItem(0).cast("int") % 24)
        .otherwise((F.floor(num(c) / 100) % 24).cast("int"))
    def flag(c: String): Column =
      F.when(F.lower(str(c)) === "true", 1).otherwise(F.when(num(c) >= 0.5, 1).otherwise(0))

    val date = F.coalesce(
      F.to_date(str("FL_DATE"), "yyyy-MM-dd"),
      F.to_date(F.split(str("FL_DATE"), " ").getItem(0), "M/d/yyyy")
    )
    val isoDow = ((F.dayofweek(F.col("FL_DATE")) + 5) % 7) + 1

    var df: DataFrame = upper
    // Cast-failure counts: a non-empty source value that did not parse as a number.
    val castFailures = NumericCols.filter(present.contains).map { c =>
      F.sum(F.when(str(c).isNotNull && str(c) =!= "" && num(c).isNull, 1).otherwise(0)).as(c)
    }

    NumericCols.filter(present.contains).foreach(c => df = df.withColumn(c, num(c)))
    df = df
      .withColumn("FL_DATE", date)
      .withColumn("CANCELLED", flag("CANCELLED"))
      .withColumn("DIVERTED", flag("DIVERTED"))
      .withColumn("DEP_HOUR", hour("CRS_DEP_TIME"))
      .withColumn("ARR_HOUR", hour("CRS_ARR_TIME"))
      .withColumn("MONTH",
        if (present("MONTH")) F.coalesce(num("MONTH").cast("int"), F.month(F.col("FL_DATE")))
        else F.month(F.col("FL_DATE")))
      .withColumn("DAY_OF_WEEK",
        if (present("DAY_OF_WEEK")) F.coalesce(num("DAY_OF_WEEK").cast("int"), isoDow) else isoDow)
      .withColumn("ROUTE",
        if (present("ROUTE")) F.coalesce(F.when(str("ROUTE") =!= "", str("ROUTE")), F.concat_ws("_", str("ORIGIN"), str("DEST")))
        else F.concat_ws("_", str("ORIGIN"), str("DEST")))
      .withColumn("OP_UNIQUE_CARRIER", str("OP_UNIQUE_CARRIER"))
      .withColumn("ORIGIN", str("ORIGIN"))
      .withColumn("DEST", str("DEST"))

    val malformed = F.col("FL_DATE").isNull ||
      F.coalesce(F.col("ORIGIN"), F.lit("")) === "" ||
      F.coalesce(F.col("DEST"), F.lit("")) === "" ||
      F.coalesce(F.col("OP_UNIQUE_CARRIER"), F.lit("")) === ""
    df = df.withColumn("_MALFORMED", malformed).cache()

    // ---------------------------------------------------------------- validation counts
    val ((rowsRead, failures), readSecs) = Common.time {
      val n = df.count()
      val r = upper.agg(castFailures.head, castFailures.tail: _*).head()
      val f = NumericCols.filter(present.contains).zipWithIndex
        .map { case (c, i) => c -> (if (r.isNullAt(i)) 0L else r.getLong(i)) }.toMap
      (n, f)
    }
    val clean = df.filter(!F.col("_MALFORMED")).drop("_MALFORMED")
    val malformedRows = df.filter(F.col("_MALFORMED")).count()
    val cancelled = clean.filter(F.col("CANCELLED") === 1).count()
    val diverted = clean.filter(F.col("DIVERTED") === 1).count()
    val validRows = rowsRead - malformedRows
    val operated = clean.filter(F.col("CANCELLED") === 0 && F.col("DIVERTED") === 0).count()
    val perMonth = clean.groupBy("MONTH").count().orderBy("MONTH").collect()
      .map(r => r.getInt(0).toString -> r.getLong(1))

    // ---------------------------------------------------------------- write Parquet
    val out = Common.processed(m)
    val (_, writeSecs) = Common.time {
      clean.repartition(F.col("MONTH")).write.mode("overwrite").partitionBy("MONTH").parquet(out)
    }
    val check = spark.read.parquet(out).count()

    say()
    say(s"=== Ingest [$m]")
    say(f"Rows read from CSV       : $rowsRead%,d   (read + cache ${readSecs}%.1f s)")
    say(f"Malformed rows dropped   : $malformedRows%,d   (unparseable FL_DATE, or empty carrier / origin / dest)")
    say(f"Valid rows               : $validRows%,d")
    say(f"  cancelled              : $cancelled%,d")
    say(f"  diverted               : $diverted%,d")
    say(f"  operated (neither)     : $operated%,d")
    say(s"Numeric cast failures    : ${failures.filter(_._2 > 0).map { case (c, n) => s"$c=$n" }.mkString(", ") match { case "" => "none"; case s => s }}")
    say(s"Rows per month           : ${perMonth.map { case (k, v) => s"$k:$v" }.mkString("  ")}")
    say(f"Parquet written          : $out  (${writeSecs}%.1f s)")
    say(f"Parquet row count        : $check%,d  ${if (check == validRows) "== valid rows (reconciled)" else "!= valid rows — INVESTIGATE"}")

    Common.writeText(s"${Common.logs}/ingest.txt", log.toString)
    Common.writeJson("ingest_summary.json", m, rowsRead, "Ingest.scala", ListMap(
      "columns_in_file" -> cols.size,
      "rows_read" -> rowsRead,
      "malformed_rows" -> malformedRows,
      "valid_rows" -> validRows,
      "cancelled" -> cancelled,
      "diverted" -> diverted,
      "operated" -> operated,
      "parquet_rows" -> check,
      "reconciled" -> (check == validRows),
      "rows_per_month" -> ListMap(perMonth: _*),
      "numeric_cast_failures" -> ListMap(failures.toSeq.sortBy(_._1): _*),
      "optional_columns_missing" -> optionalMissing,
      "parquet_path" -> out.stripPrefix(Common.Root + "/")
    ))
    spark.stop()
  }
}
