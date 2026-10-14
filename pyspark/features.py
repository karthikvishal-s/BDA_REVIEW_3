"""Leakage-safe feature construction for both models (corrected Chapter 5).

Model 1  regression      ARR_DELAY in minutes, all operated flights
Model 2  classification  dominant delay cause, flights with ARR_DELAY >= 15

Each model is trained on two feature sets:
  pre    schedule-time information only (known when the timetable is published)
  atdep  pre + DEP_DELAY and TAXI_OUT (known at pushback / wheels-off)

Rules enforced here (doc §5.2 - §5.5):
  * temporal split — train on the earlier months, test on the later ones;
  * every historical (target) encoding is computed on the TRAIN split only and joined onto both;
  * assert_no_leakage() refuses any post-arrival column, and any at-departure column in `pre`.

Writes data/features/<mode>/{train,test}.parquet and outputs/ml/feature_report.json.
"""
from functools import reduce

from pyspark.sql import DataFrame, functions as F

import common
from common import CAUSES

SMOOTHING = 50  # m in (sum + m * global) / (count + m)

# --------------------------------------------------------------------------- leakage contract
POST_ARRIVAL = {
    "ARR_DELAY", "ARR_DELAY_NEW", "ARR_DEL15", "ARR_DELAY_CLIPPED", "DELAY_CATEGORY",
    "ARR_TIME", "AIR_TIME", "TAXI_IN", "WHEELS_ON", "ACTUAL_ELAPSED_TIME",
    "DOMINANT_CAUSE", "CAUSE_STATUS", "LABEL", *CAUSES,
}
AT_DEPARTURE = {"DEP_DELAY", "DEP_DELAY_NEW", "DEP_DEL15", "TAXI_OUT", "DEP_TIME", "WHEELS_OFF"}
FILTER_ONLY = {"CANCELLED", "DIVERTED", "CANCELLATION_CODE"}

CATEGORICAL = ["OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "DEP_TIME_CATEGORY", "DISTANCE_CATEGORY"]
NUMERIC = ["MONTH", "DAY_OF_WEEK", "DEP_HOUR", "ARR_HOUR", "DISTANCE", "ORIGIN_HOUR_DEPARTURES"]
AT_DEP_EXTRA = ["DEP_DELAY", "TAXI_OUT"]

REG_ENCODINGS = {
    "HIST_ROUTE_MEAN_DELAY": ["ROUTE"],
    "HIST_CARRIER_MEAN_DELAY": ["OP_UNIQUE_CARRIER"],
    "HIST_ORIGIN_MEAN_DELAY": ["ORIGIN"],
    "HIST_DEST_MEAN_DELAY": ["DEST"],
    "HIST_ORIGIN_HOUR_MEAN_DELAY": ["ORIGIN", "DEP_HOUR"],
}
REG_RATE_ENCODINGS = {"HIST_ROUTE_LATE_RATE": ["ROUTE"], "HIST_CARRIER_LATE_RATE": ["OP_UNIQUE_CARRIER"]}
CAUSE_KEYS = {"CARRIER": ["OP_UNIQUE_CARRIER"], "ORIGIN": ["ORIGIN"], "ROUTE": ["ROUTE"]}


def short(cause):
    return cause.replace("_DELAY", "")


CAUSE_ENCODINGS = [f"HIST_{k}_SHARE_{short(c)}" for k in CAUSE_KEYS for c in CAUSES]


def assert_no_leakage(columns, task, feature_set):
    """Raise if a column that is unknown at prediction time would reach the model."""
    bad = sorted(c for c in columns if c in POST_ARRIVAL or c in FILTER_ONLY)
    if feature_set == "pre":
        bad += sorted(c for c in columns if c in AT_DEPARTURE)
    if bad:
        raise ValueError(f"Leakage: {bad} not allowed as features for {task}/{feature_set}")


def feature_columns(task, feature_set, available):
    """(categorical, numeric) feature lists for one model, restricted to columns that exist."""
    if task not in ("regression", "cause") or feature_set not in ("pre", "atdep"):
        raise ValueError(f"unknown task/feature set {task}/{feature_set}")
    cats = [c for c in CATEGORICAL if c in available]
    nums = [c for c in NUMERIC if c in available]
    nums += list(REG_ENCODINGS) + list(REG_RATE_ENCODINGS) if task == "regression" else CAUSE_ENCODINGS
    if feature_set == "atdep":
        nums += [c for c in AT_DEP_EXTRA if c in available]
    assert_no_leakage(cats + nums, task, feature_set)
    return cats, nums


# --------------------------------------------------------------------------- building blocks
def add_dominant_cause(df: DataFrame) -> DataFrame:
    """DOMINANT_CAUSE = argmax of the five cause columns; CAUSE_STATUS explains every null."""
    reported = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in CAUSES])
    mx = F.greatest(*[F.col(c) for c in CAUSES])
    n_max = sum(F.when(F.col(c) == mx, 1).otherwise(0) for c in CAUSES)
    argmax = F.coalesce(*[F.when(F.col(c) == mx, F.lit(c)) for c in CAUSES])
    status = (
        F.when(F.col("ARR_DELAY") < 15, "not_delayed")
        .when(~reported, "causes_missing")
        .when(mx <= 0, "zero_causes")
        .when(n_max > 1, "tie")
        .otherwise("ok")
    )
    return (df.withColumn("CAUSE_STATUS", status)
              .withColumn("DOMINANT_CAUSE", F.when(F.col("CAUSE_STATUS") == "ok", argmax)))


def temporal_split(df: DataFrame, train_months, test_months):
    overlap = set(train_months) & set(test_months)
    if overlap:
        raise ValueError(f"train and test months overlap: {sorted(overlap)}")
    return (df.filter(F.col("MONTH").isin(list(train_months))),
            df.filter(F.col("MONTH").isin(list(test_months))))


def smoothed_means(source: DataFrame, keys, values, m=SMOOTHING):
    """Per-key smoothed mean of each value expression, from `source` only.

    values: {output_name: Column}. Returns (stats DataFrame [keys + outputs], {output: global mean}).
    Keys never seen in `source` later receive the global mean through attach().
    """
    glob = source.agg(*[F.avg(e).alias(n) for n, e in values.items()]).first()
    g = {n: float(glob[n]) if glob[n] is not None else 0.0 for n in values}
    sums = source.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("_n"), *[F.sum(e).alias(f"_s_{n}") for n, e in values.items()])
    stats = sums.select(*keys, *[
        ((F.col(f"_s_{n}") + F.lit(m * g[n])) / (F.col("_n") + F.lit(m))).alias(n) for n in values])
    return stats, g


def attach(df: DataFrame, stats: DataFrame, keys, g) -> DataFrame:
    return df.join(F.broadcast(stats), on=keys, how="left").fillna(g)


# --------------------------------------------------------------------------- main build
def build(spark, mode, train_months=common.TRAIN_MONTHS, test_months=common.TEST_MONTHS):
    common.require_processed(mode)
    flights = spark.read.parquet(common.processed(mode))
    available = set(flights.columns)
    scheduled = flights.count()

    # Congestion proxy: scheduled departures from the same airport in the same hour of the same day.
    # Uses the timetable only (cancelled flights included), so it is known in advance.
    congestion = flights.groupBy("FL_DATE", "ORIGIN", "DEP_HOUR").agg(
        F.count(F.lit(1)).alias("ORIGIN_HOUR_DEPARTURES"))
    flights = flights.join(congestion, ["FL_DATE", "ORIGIN", "DEP_HOUR"], "left")

    operated = flights.filter((F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 0)
                              & F.col("ARR_DELAY").isNotNull())
    operated = add_dominant_cause(operated)
    for c in ("DEP_TIME_CATEGORY", "DISTANCE_CATEGORY"):
        if c in available:
            operated = operated.withColumn(c, F.coalesce(F.col(c), F.lit("UNKNOWN")))

    train, test = temporal_split(operated, train_months, test_months)
    train, test = train.cache(), test.cache()
    n_train, n_test = train.count(), test.count()
    if n_train == 0 or n_test == 0:
        raise SystemExit(f"Empty split (train={n_train}, test={n_test}). Months present: "
                         f"{sorted(r[0] for r in operated.select('MONTH').distinct().collect())}")

    # Winsorise the regression training label at the train 0.1% / 99.9% quantiles.
    lo, hi = train.approxQuantile("ARR_DELAY", [0.001, 0.999], 0.0005)
    clip = F.least(F.greatest(F.col("ARR_DELAY"), F.lit(lo)), F.lit(hi))
    train = train.withColumn("ARR_DELAY_CLIPPED", clip)
    test = test.withColumn("ARR_DELAY_CLIPPED", clip)

    # Impute numeric gaps with TRAIN medians (counted, never silently).
    numeric_present = [c for c in NUMERIC + AT_DEP_EXTRA if c in available or c == "ORIGIN_HOUR_DEPARTURES"]
    medians = dict(zip(numeric_present, train.approxQuantile(numeric_present, [0.5], 0.001)))
    medians = {c: float(v[0]) if v else 0.0 for c, v in medians.items()}
    nulls = train.agg(*[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in numeric_present]).first().asDict()
    train, test = train.fillna(medians), test.fillna(medians)

    # Model 1 encodings: smoothed mean (clipped) arrival delay and late rate per key, TRAIN only.
    for name, keys in REG_ENCODINGS.items():
        stats, g = smoothed_means(train, keys, {name: F.col("ARR_DELAY_CLIPPED")})
        train, test = attach(train, stats, keys, g), attach(test, stats, keys, g)
    for name, keys in REG_RATE_ENCODINGS.items():
        stats, g = smoothed_means(train, keys, {name: (F.col("ARR_DELAY") >= 15).cast("double")})
        train, test = attach(train, stats, keys, g), attach(test, stats, keys, g)

    # Model 2 encodings: historical share of each dominant cause per key, from TRAIN late flights only.
    cause_train = train.filter(F.col("DOMINANT_CAUSE").isNotNull())
    for k, keys in CAUSE_KEYS.items():
        values = {f"HIST_{k}_SHARE_{short(c)}": (F.col("DOMINANT_CAUSE") == c).cast("double") for c in CAUSES}
        stats, g = smoothed_means(cause_train, keys, values)
        train, test = attach(train, stats, keys, g), attach(test, stats, keys, g)

    keep_ids = ["FL_DATE", "OP_CARRIER_FL_NUM", "ROUTE"]
    labels = ["ARR_DELAY", "ARR_DELAY_CLIPPED", "DOMINANT_CAUSE", "CAUSE_STATUS"]
    cols = []
    for c in keep_ids + CATEGORICAL + NUMERIC + AT_DEP_EXTRA + list(REG_ENCODINGS) + \
            list(REG_RATE_ENCODINGS) + CAUSE_ENCODINGS + labels:
        if c not in cols and (c in train.columns):
            cols.append(c)
    train, test = train.select(*cols), test.select(*cols)

    out = common.features_dir(mode)
    train.write.mode("overwrite").parquet(f"{out}/train.parquet")
    test.write.mode("overwrite").parquet(f"{out}/test.parquet")
    # Report from the written files rather than re-running the whole join lineage.
    train = spark.read.parquet(f"{out}/train.parquet")
    test = spark.read.parquet(f"{out}/test.parquet")

    def counts(df, col):
        return {r[0]: r[1] for r in df.groupBy(col).count().orderBy(col).collect() if r[0] is not None}

    report = {
        "split": {"train_months": list(train_months), "test_months": list(test_months),
                  "train_rows": n_train, "test_rows": n_test},
        "scheduled_flights": scheduled,
        "operated_with_arrival": n_train + n_test,
        "removed_cancelled_diverted_or_no_arrival": scheduled - n_train - n_test,
        "regression": {
            "label": "ARR_DELAY (minutes)",
            "train_label_winsorised_to": [lo, hi],
            "train_rows": n_train, "test_rows": n_test,
        },
        "cause": {
            "label": "DOMINANT_CAUSE = argmax of the five cause columns",
            "cause_status_train": counts(train, "CAUSE_STATUS"),
            "cause_status_test": counts(test, "CAUSE_STATUS"),
            "class_distribution_train": counts(train, "DOMINANT_CAUSE"),
            "class_distribution_test": counts(test, "DOMINANT_CAUSE"),
        },
        "imputed_with_train_median": {c: {"train_nulls": int(nulls[c] or 0), "median": medians[c]}
                                      for c in numeric_present},
        "smoothing_m": SMOOTHING,
        "feature_sets": {f"{t}_{fs}": dict(zip(("categorical", "numeric"), feature_columns(t, fs, set(cols))))
                         for t in ("regression", "cause") for fs in ("pre", "atdep")},
    }
    common.write_json("outputs/ml/feature_report.json", report, mode, scheduled, "features.py")
    return report


def parse_months(s):
    a, _, b = s.partition("-")
    return list(range(int(a), int(b or a) + 1))


def main():
    p = common.parser(__doc__.splitlines()[0])
    p.add_argument("--train-months", default="1-9")
    p.add_argument("--test-months", default="10-12")
    args = p.parse_args()
    spark = common.spark_session("Features", args.input)
    r = build(spark, args.input, parse_months(args.train_months), parse_months(args.test_months))
    print(f"train={r['split']['train_rows']:,}  test={r['split']['test_rows']:,}")
    print("cause classes (train):", r["cause"]["class_distribution_train"])
    spark.stop()


if __name__ == "__main__":
    main()
