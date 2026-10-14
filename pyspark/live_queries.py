"""Parameterised queries for the live 15-minute analysis question (doc §9.1).

  make query Q='top-routes --n 10 --month 7'
  make query Q='carrier-delays --carrier AA'
  make query Q='busiest-hour --airport ATL'
  make query Q='compare-carriers --route ATL_LGA --carriers DL AA'
  make query Q='cancellations --month 1'
  make query Q='cause-mix --airport ORD'
  make query Q='sql --q "SELECT ORIGIN, count(*) n FROM flights GROUP BY ORIGIN ORDER BY n DESC LIMIT 5"'

Every query runs against the processed Parquet for INPUT (default sample), and prints its wall time.
"""
import time

from pyspark.sql import functions as F

import common
from common import CAUSES

# Functions, not constants: Column expressions need an active SparkContext.
def late():
    return (F.col("ARR_DELAY") >= 15).cast("double")


def operated():
    return (F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 0) & F.col("ARR_DELAY").isNotNull()


def top_routes(df, a):
    d = df.filter(operated())
    if a.month:
        d = d.filter(F.col("MONTH") == a.month)
    return (d.groupBy("ROUTE").agg(F.count(F.lit(1)).alias("flights"), F.round(F.avg(late()) * 100, 1).alias("late_pct"),
                                   F.round(F.avg("ARR_DELAY"), 1).alias("avg_delay"))
            .filter(F.col("flights") >= a.min_flights).orderBy(F.desc("late_pct")).limit(a.n))


def carrier_delays(df, a):
    d = df.filter(operated() & (F.col("OP_UNIQUE_CARRIER") == a.carrier.upper()))
    band = (F.when(F.col("ARR_DELAY") < 0, "1 early").when(F.col("ARR_DELAY") < 15, "2 on time (0-14)")
            .when(F.col("ARR_DELAY") < 60, "3 late 15-59").when(F.col("ARR_DELAY") < 180, "4 late 60-179")
            .otherwise("5 late 180+"))
    total = d.count()
    return (d.groupBy(band.alias("band")).agg(F.count(F.lit(1)).alias("flights"))
            .withColumn("pct", F.round(F.col("flights") * 100 / F.lit(max(total, 1)), 2)).orderBy("band"))


def busiest_hour(df, a):
    return (df.filter(F.col("ORIGIN") == a.airport.upper()).groupBy("DEP_HOUR")
            .agg(F.count(F.lit(1)).alias("departures"), F.round(F.avg(F.when(operated(), late())) * 100, 1).alias("late_pct"))
            .orderBy(F.desc("departures")).limit(a.n))


def compare_carriers(df, a):
    d = df.filter((F.col("ROUTE") == a.route.upper()) & F.col("OP_UNIQUE_CARRIER").isin([c.upper() for c in a.carriers]))
    return (d.groupBy("OP_UNIQUE_CARRIER").agg(
        F.count(F.lit(1)).alias("scheduled"), F.sum("CANCELLED").alias("cancelled"),
        F.round(F.avg(F.when(operated(), F.col("ARR_DELAY"))), 1).alias("avg_delay"),
        F.round(F.avg(F.when(operated(), late())) * 100, 1).alias("late_pct")))


def cancellations(df, a):
    d = df if not a.month else df.filter(F.col("MONTH") == a.month)
    return (d.groupBy("OP_UNIQUE_CARRIER").agg(F.count(F.lit(1)).alias("scheduled"), F.sum("CANCELLED").alias("cancelled"))
            .withColumn("cancel_pct", F.round(F.col("cancelled") * 100 / F.col("scheduled"), 2))
            .orderBy(F.desc("cancel_pct")))


def cause_mix(df, a):
    d = df.filter(operated() & (F.col("ARR_DELAY") >= 15))
    if a.airport:
        d = d.filter(F.col("ORIGIN") == a.airport.upper())
    tot = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in CAUSES)
    return d.agg(F.count(F.lit(1)).alias("late_flights"),
                 *[F.round(F.sum(c) * 100 / F.sum(tot), 1).alias(c.replace("_DELAY", "_pct")) for c in CAUSES])


def sql(df, a):
    df.createOrReplaceTempView("flights")
    return df.sparkSession.sql(a.q)


def main():
    p = common.parser("Live analysis queries")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("top-routes"); s.add_argument("--n", type=int, default=10)
    s.add_argument("--month", type=int); s.add_argument("--min-flights", type=int, default=50)
    s = sub.add_parser("carrier-delays"); s.add_argument("--carrier", required=True)
    s = sub.add_parser("busiest-hour"); s.add_argument("--airport", required=True); s.add_argument("--n", type=int, default=5)
    s = sub.add_parser("compare-carriers"); s.add_argument("--route", required=True)
    s.add_argument("--carriers", nargs="+", required=True)
    s = sub.add_parser("cancellations"); s.add_argument("--month", type=int)
    s = sub.add_parser("cause-mix"); s.add_argument("--airport")
    s = sub.add_parser("sql"); s.add_argument("--q", required=True)
    a = p.parse_args()

    common.require_processed(a.input)
    spark = common.spark_session(f"Query {a.cmd}", a.input)
    df = spark.read.parquet(common.processed(a.input))
    fn = {"top-routes": top_routes, "carrier-delays": carrier_delays, "busiest-hour": busiest_hour,
          "compare-carriers": compare_carriers, "cancellations": cancellations, "cause-mix": cause_mix,
          "sql": sql}[a.cmd]
    t0 = time.time()
    fn(df, a).show(50, truncate=False)
    print(f"[{a.cmd}] on INPUT={a.input}: {time.time() - t0:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
