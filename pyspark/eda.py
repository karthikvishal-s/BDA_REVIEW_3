"""Exploratory analysis on the processed Parquet: distributions, missingness and cause behaviour.

Writes outputs/ml/eda.json. The cause-by-month and cause-by-hour tables are what motivate the
historical cause-share features of Model 2 (weather is seasonal, late aircraft builds up
through the day).
"""
from pyspark.sql import functions as F

import common
from common import CAUSES
from features import add_dominant_cause


def main():
    args = common.parser(__doc__.splitlines()[0]).parse_args()
    mode = args.input
    common.require_processed(mode)
    spark = common.spark_session("EDA", mode)
    df = spark.read.parquet(common.processed(mode)).cache()
    n = df.count()

    operated = df.filter((F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 0) & F.col("ARR_DELAY").isNotNull())
    n_op = operated.count()

    # ARR_DELAY distribution
    qs = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 0.999]
    qv = operated.approxQuantile("ARR_DELAY", qs, 0.0005)
    s = operated.agg(
        F.avg("ARR_DELAY").alias("mean"), F.stddev("ARR_DELAY").alias("std"),
        F.min("ARR_DELAY").alias("min"), F.max("ARR_DELAY").alias("max"),
        *[F.avg((F.col("ARR_DELAY") >= t).cast("double")).alias(f"share_ge_{t}") for t in (15, 60, 180)],
        F.avg((F.col("ARR_DELAY") < 0).cast("double")).alias("share_early"),
    ).first().asDict()
    clipped = F.least(F.greatest(F.col("ARR_DELAY"), F.lit(-60.0)), F.lit(300.0))
    hist = (operated.groupBy((F.floor(clipped / 10) * 10).cast("int").alias("bin")).count()
            .orderBy("bin").collect())

    # Missingness per column (nulls, and empty strings for text columns)
    exprs = []
    for f in df.schema.fields:
        c = F.col(f.name)
        miss = c.isNull() | (F.trim(c) == "") if f.dataType.simpleString() == "string" else c.isNull()
        exprs.append(F.sum(miss.cast("long")).alias(f.name))
    missing = df.agg(*exprs).first().asDict()

    # Cause behaviour among late flights
    late = add_dominant_cause(operated.filter(F.col("ARR_DELAY") >= 15))
    status = {r[0]: r[1] for r in late.groupBy("CAUSE_STATUS").count().collect()}
    labelled = late.filter(F.col("DOMINANT_CAUSE").isNotNull())

    def share_by(col):
        rows = labelled.groupBy(col, "DOMINANT_CAUSE").count().collect()
        tot = {}
        for r in rows:
            tot[r[0]] = tot.get(r[0], 0) + r[2]
        out = {}
        for r in rows:
            out.setdefault(r[0], {c: 0.0 for c in CAUSES})[r[1]] = r[2] / tot[r[0]]
        return [{col.lower(): k, **v, "n": tot[k]} for k, v in sorted(out.items()) if k is not None]

    common.write_json("outputs/ml/eda.json", {
        "rows": n,
        "operated_with_arrival": n_op,
        "arr_delay": {
            "quantiles": dict(zip([str(q) for q in qs], qv)),
            **{k: v for k, v in s.items()},
            "histogram_10min_clipped_-60_300": [{"bin": r[0], "count": r[1]} for r in hist],
        },
        "missing_by_column": {k: int(v or 0) for k, v in missing.items()},
        "late_flights_cause_status": status,
        "dominant_cause_share_by_month": share_by("MONTH"),
        "dominant_cause_share_by_dep_hour": share_by("DEP_HOUR"),
    }, mode, n, "eda.py")

    print(f"rows={n:,} operated={n_op:,} mean ARR_DELAY={s['mean']:.2f} median={qv[3]:.1f} "
          f"late(>=15)={100 * s['share_ge_15']:.1f}%")
    print("late-flight cause status:", status)
    spark.stop()


if __name__ == "__main__":
    main()
