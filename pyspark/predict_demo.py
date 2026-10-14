"""Prediction demo: chain the two saved PRE-departure models on held-out test flights.

For each flight: predicted arrival delay (minutes), whether that crosses the 15-minute line, and
the cause model's "likely cause if late" with its probability, side by side with what actually
happened. 10 flights that were actually late and 10
that were not are drawn at random (seeded) from the test months.
"""
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

import common
from common import CAUSES


def best(metrics, task, fs, key, higher_is_better):
    models = metrics.get(task, {}).get("models", {}).get(fs, {})
    if not models:
        return "rf"
    pick = max if higher_is_better else min
    return pick(models, key=lambda a: models[a][key])


def main():
    p = common.parser(__doc__.splitlines()[0])
    p.add_argument("--feature-set", default="pre", choices=["pre", "atdep"])
    p.add_argument("--n", type=int, default=10, help="flights per group (late / not late)")
    args = p.parse_args()
    mode, fs = args.input, args.feature_set
    spark = common.spark_session("PredictDemo", mode)

    try:
        metrics = common.read_json("outputs/ml/metrics.json")
    except FileNotFoundError:
        metrics = {}
    reg_algo = best(metrics, "regression", fs, "mae", False)
    cause_algo = best(metrics, "cause", fs, "macro_f1", True)
    reg = PipelineModel.load(f"{common.models_dir(mode)}/regression_{fs}_{reg_algo}")
    cause = PipelineModel.load(f"{common.models_dir(mode)}/cause_{fs}_{cause_algo}")

    test = spark.read.parquet(f"{common.features_dir(mode)}/test.parquet")
    late = test.filter(F.col("DOMINANT_CAUSE").isNotNull()).orderBy(F.rand(11)).limit(args.n)
    ok = test.filter(F.col("ARR_DELAY") < 15).orderBy(F.rand(12)).limit(args.n)
    # Materialise once so both models score exactly the same rows with the same ids.
    sample = late.unionByName(ok).withColumn("_ROW", F.monotonically_increasing_id()).cache()
    sample.count()

    with_minutes = reg.transform(sample).select("_ROW", F.col("prediction").alias("PRED_MINUTES"))
    labels = F.array(*[F.lit(c) for c in CAUSES])
    # The cause model answers "if this flight is late, why?" — shown for every flight with its
    # probability. FLAGGED_LATE says whether the minutes model itself predicts >= 15 minutes.
    with_cause = cause.transform(sample).select(
        "_ROW", F.element_at(labels, F.col("prediction").cast("int") + 1).alias("PRED_CAUSE"),
        F.array_max(vector_to_array("probability")).alias("PRED_CAUSE_PROB"))

    rows = (sample.join(with_minutes, "_ROW").join(with_cause, "_ROW")
            .withColumn("FLAGGED_LATE", F.col("PRED_MINUTES") >= 15)
            .select("FL_DATE", "OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "ROUTE", "DEP_HOUR",
                    "ARR_DELAY", "PRED_MINUTES", "FLAGGED_LATE", "DOMINANT_CAUSE", "PRED_CAUSE",
                    "PRED_CAUSE_PROB")
            .orderBy(F.col("ARR_DELAY").desc()).collect())

    print(f"\nPrediction demo [{mode}] feature set={fs}  regression={reg_algo}  cause={cause_algo}")
    print(f"{'date':10} {'flight':9} {'route':9} {'hr':>2} {'actual':>7} {'pred':>7} {'flag':>4}  "
          f"{'actual cause':20} {'likely cause if late':20} {'p':>4}")
    out = []
    for r in rows:
        print(f"{str(r['FL_DATE']):10} {r['OP_UNIQUE_CARRIER']}{str(r['OP_CARRIER_FL_NUM']):>6} "
              f"{r['ROUTE']:9} {int(r['DEP_HOUR']):2d} {r['ARR_DELAY']:7.0f} {r['PRED_MINUTES']:7.1f} "
              f"{'LATE' if r['FLAGGED_LATE'] else '':>4}  {r['DOMINANT_CAUSE'] or '-':20} "
              f"{r['PRED_CAUSE']:20} {r['PRED_CAUSE_PROB']:4.2f}")
        out.append({k: (str(v) if k == "FL_DATE" else v) for k, v in r.asDict().items()})
    n_late = sum(1 for r in rows if r["DOMINANT_CAUSE"])
    hits = sum(1 for r in rows if r["DOMINANT_CAUSE"] and r["DOMINANT_CAUSE"] == r["PRED_CAUSE"])
    flagged = sum(1 for r in rows if r["FLAGGED_LATE"] and r["ARR_DELAY"] >= 15)
    print(f"\nlate flights flagged by the minutes model: {flagged}/{n_late}; "
          f"cause correct on late flights: {hits}/{n_late}")

    common.write_json("outputs/ml/predict_demo.json",
                      {"feature_set": fs, "regression_model": reg_algo, "cause_model": cause_algo,
                       "flights": out}, mode, len(out), "predict_demo.py")
    spark.stop()


if __name__ == "__main__":
    main()
