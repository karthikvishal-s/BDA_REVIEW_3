# Flight Delay & Route Optimisation — Review 3
**23CSE352 Big Data Analytics** · Apache Spark, Scala RDD, PySpark MLlib

This continues the Review 1 (Hadoop / Hive) and Review 2 (HBase) work on the same BTS on-time
performance data: `flight_data_cleaned_1year.csv`, 7,001,620 flights, 38 columns.

```
data/raw/flight_data_cleaned_1year.csv
      │  Ingest.scala         CSV -> Parquet (partitioned by MONTH) + validation counts
      ▼
data/processed/<mode>/flights.parquet
      ├── RddDemo.scala        10-mark RDD module -> outputs/logs/{rdd_demo,lineage,partitions,timings}.txt
      ├── RouteAnalytics.scala pair-RDD analytics -> outputs/scala/*.json
      ├── eda.py               distributions, missingness, cause mix -> outputs/ml/eda.json
      └── features.py          leakage-safe features, temporal split -> data/features/<mode>/
              ├── train.py     8 models -> models/<mode>/
              ├── evaluate.py  metrics vs baselines -> outputs/ml/metrics.json
              └── predict_demo.py  minutes -> cause, on test flights
viz/make_plots.py  reads outputs/ -> outputs/plots/*.png        api/ (optional dashboard)
```

## The two ML models
1. **Regression:** predict **arrival delay in minutes** (`ARR_DELAY`).
2. **Multi-class classification:** predict the **dominant cause** of a delay: `CARRIER_DELAY`,
   `WEATHER_DELAY`, `NAS_DELAY`, `SECURITY_DELAY` or `LATE_AIRCRAFT_DELAY`.

Each is trained with LR and RF on two feature sets: **pre-departure** (schedule only) and
**at-departure** (adds `DEP_DELAY`, `TAXI_OUT`). [docs/ML_DESIGN.md](docs/ML_DESIGN.md) covers the
leakage table, the temporal split and the train-only encodings.

## Quick start
```bash
make build && make smoke         # full chain on a SYNTHETIC fixture — checks the setup only
cp …/flight_data_cleaned_1year.csv data/raw/
make schema-check && make sample
make all INPUT=full              # the run whose numbers are reported
make rdd INPUT=full HOLD=300     # rerun with the Spark UI held open on :4040 for screenshots
```
`INPUT` is `full`, `sample` (100k real rows, the default) or `synthetic`. Every JSON output records
its input in `_meta`, and every chart from a non-full run carries a red banner. Only `full`
outputs go in the report.

**Trained models are in the repo** (`models/full/`, the 8 pipelines from the full-data run). To
evaluate or demo them without the ~10 min retrain:
```bash
make ingest features evaluate predict INPUT=full   # rebuilds the test features, reuses the committed models
```
The test features must still be rebuilt locally, because `data/` is not in Git. They reproduce
exactly: the split and the encodings are deterministic.

| Target | Does |
|---|---|
| `make ingest / rdd / analytics` | Scala jobs (the JAR is rebuilt automatically when sources change) |
| `make eda / features / train / evaluate / predict` | PySpark stages (`TASK=regression\|cause\|both`) |
| `make plots` | All charts from `outputs/` |
| `make test` | Unit tests: labels, leakage guard, train-only encodings, split |
| `make query Q='…'` | Live analysis queries ([docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md)) |
| `make history` / `make api` | Spark history server :18080 / optional dashboard :8000 |

## Team and ownership
| Member | Roll No. | Area | Main files |
|---|---|---|---|
| Karthik Vishal S | CB.SC.U4CSE23239 | Scala Spark and RDD operations | `RddDemo.scala`, `RouteAnalytics.scala` |
| Darun R G | CB.SC.U4CSE23215 | Ingestion, EDA, Spark execution evidence | `Ingest.scala`, `eda.py`, Spark UI, cache study |
| Abhinav G | CB.SC.U4CSE23201 | PySpark ML: both models | `features.py`, `train.py`, `evaluate.py`, `predict_demo.py` |
| Sathya Roopan M | CB.SC.U4CSE23256 | Visualisation, dashboard, integration | `make_plots.py`, `api/`, demo script |

Every member must be able to run the whole pipeline and explain any part of it.

## Docs
[SETUP](docs/SETUP.md) · [RUBRIC_CHECKLIST](docs/RUBRIC_CHECKLIST.md) · [ML_DESIGN](docs/ML_DESIGN.md) ·
[DEMO_SCRIPT](docs/DEMO_SCRIPT.md) · [TROUBLESHOOTING](docs/TROUBLESHOOTING.md)

**Pinned:** `spark:3.5.3-scala2.12-java11-python3-ubuntu` → Spark 3.5.3, Scala 2.12.18, OpenJDK 11.0.25,
Python 3.8.10, sbt 1.10.2.
# BDA_REVIEW_3
