# Rubric checklist — 15 marks

Every item points to a file and to the command that regenerates its evidence.
Run with `INPUT=full` before the review. Every output file's `_meta.mode` must read `full`.

## Scala + Spark RDD — 10 marks (Karthik, Darun)

| Rubric item | Where | Evidence | Command |
|---|---|---|---|
| RDD creation | `RddDemo.scala` §1 `sc.textFile` | `outputs/logs/rdd_demo.txt` | `make rdd` |
| Transformation: map | §2 parse to `Flight` | 5 parsed records printed | `make rdd` |
| Transformation: filter | §3 malformed / cancelled / diverted | before and after counts | `make rdd` |
| Transformation: flatMap | §4 one token per cause | cause frequency table | `make rdd` |
| Transformation: reduceByKey | §5 route sums | distinct routes | `make rdd` |
| Transformation: mapValues | §6 averages | top 10 routes | `make rdd` |
| Actions: count / take / reduce | §7 | counts, sample keys, total minutes | `make rdd` |
| Key-value operations | §8 `aggregateByKey`, `join`, dominant cause | carrier table, `outputs/scala/dominant_cause_counts.json` | `make rdd` |
| Partitions | §9 `getNumPartitions`, `repartition`, `coalesce` | `outputs/logs/partitions.txt` | `make rdd` |
| Lineage | §10 `toDebugString` | `outputs/logs/lineage.txt` | `make rdd` |
| DAG | Spark UI during the run, plus the event log | screenshots in `docs/screenshots/`; history server | `make rdd HOLD=300` then open :4040, or `make history` → :18080 |
| Fault tolerance | §10 explanation grounded in the real lineage | `outputs/logs/lineage.txt` | `make rdd` |
| cache / persist | §11, four timed runs | `outputs/logs/timings.txt` | `make rdd` |
| Ingestion + validation | `Ingest.scala` | `outputs/logs/ingest.txt`, `outputs/scala/ingest_summary.json` | `make ingest` |
| Pair-RDD analytics | `RouteAnalytics.scala` | `outputs/scala/*.json` | `make analytics` |

## PySpark ML — 3 marks (Abhinav)

| Rubric item | Where | Evidence |
|---|---|---|
| Preprocessing | `features.py`: filters, imputation, dominant-cause label | `outputs/ml/feature_report.json` |
| Feature engineering | `features.py`: train-only encodings, congestion, two feature sets | `feature_report.json` → `feature_sets` |
| Train / test split | Temporal: months 1–9 / 10–12 | `feature_report.json` → `split` |
| Models | `train.py`: LR + RF for **regression** (ARR_DELAY minutes) and for the **cause classifier** | `models/<mode>/`, `outputs/ml/train_log.json` |
| Prediction | `predict_demo.py` chains both models on test flights | `outputs/ml/predict_demo.json` |
| Evaluation | `evaluate.py`: RMSE / MAE / R² against baselines; accuracy / macro-F1 / per-class / confusion against majority | `outputs/ml/metrics.json` |
| Leakage discipline | `assert_no_leakage`, `tests/test_features.py` | `make test` |

## Visualisation and interpretation — 2 marks (Sathya)

| Chart | File | Source |
|---|---|---|
| Top 15 routes by average delay | `outputs/plots/01_top_routes.png` | `outputs/scala/route_stats.json` |
| Month / weekday / hour trend | `02_temporal_delay_rate.png` | `outputs/scala/temporal.json` |
| Cause breakdown and dominant-cause share | `03_delay_causes.png` | `outputs/scala/causes.json` |
| Predicted vs actual (Model 1) | `04_regression_pred_vs_actual.png` | `outputs/ml/regression_residuals.json` |
| Residual distribution (Model 1) | `05_regression_residuals.png` | same |
| MAE / RMSE / R² against baselines | `06_regression_metrics.png` | `outputs/ml/metrics.json` |
| Confusion matrix (Model 2) | `07_cause_confusion_matrix.png` | same |
| Per-class F1 against baseline | `08_cause_per_class_f1.png` | same |
| Feature importance (both PRE forests) | `09_regression_pre_importance.png`, `10_cause_pre_importance.png` | `outputs/ml/*_pre_rf_importance.json` |

`make plots` regenerates every chart. Each chart's footer names its source file, input and row count.

## Before the review

- [ ] `make all INPUT=full` ran on at least two laptops from a clean clone
- [ ] Every number on a slide traces to a file in `outputs/` produced in `full` mode
- [ ] Spark UI screenshots come from a real run, with the app name and timestamp visible
- [ ] The RF training fraction for Model 1 is quoted next to its metrics
- [ ] Every member has run `make query …` at least once
