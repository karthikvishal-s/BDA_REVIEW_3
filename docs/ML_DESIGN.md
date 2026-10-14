# Machine Learning Design — 3 marks (replaces Chapter 5 of the plan document)

## 1. Two prediction problems

| | Model 1 — Regression | Model 2 — Multi-class classification |
|---|---|---|
| Question | How many minutes late will this flight arrive? | Given that the flight is late, what will be the **dominant** cause? |
| Label | `ARR_DELAY` (minutes, can be negative = early) | `DOMINANT_CAUSE` = argmax of `CARRIER_DELAY`, `WEATHER_DELAY`, `NAS_DELAY`, `SECURITY_DELAY`, `LATE_AIRCRAFT_DELAY` |
| Population | Operated flights (not cancelled, not diverted) with a recorded arrival delay | Operated flights with `ARR_DELAY ≥ 15`, all five cause columns reported, cause minutes > 0, no exact tie |
| Algorithms | `LinearRegression` (elastic net), `RandomForestRegressor` | `LogisticRegression` (multinomial), `RandomForestClassifier` |
| Use | Crew / gate planning, warning connecting passengers | Routing a predicted delay to the right team (ops, maintenance, ATC liaison) |

The two models chain naturally (`pyspark/predict_demo.py`): predict minutes, and when the
prediction is ≥ 15 minutes, predict the likely cause.

**Why "dominant" cause?** BTS splits a late flight's delay minutes across the five cause columns
(they sum to `ARR_DELAY`). Picking the largest turns that into one class per flight. Flights
where two causes have exactly equal minutes are **dropped and counted** (`CAUSE_STATUS = tie`)
rather than broken arbitrarily. `outputs/ml/feature_report.json` gives the count of every
excluded status.

## 2. Two feature sets per model

| Feature set | Contents | What it simulates |
|---|---|---|
| `pre` | Schedule-time information only | Prediction when the timetable is published |
| `atdep` | `pre` + `DEP_DELAY`, `TAXI_OUT` | Prediction at pushback / wheels-off |

So there are 2 tasks × 2 feature sets × 2 algorithms = **8 trained models**. Expect `atdep` to
score far higher on Model 1, because arrival delay is mostly departure delay carried forward.
That gap is the point. A high score means nothing until you know what information the model
was given. The honest result is the `pre` model.

## 3. Leakage classification

Enforced in code by `features.assert_no_leakage()`, which raises if a forbidden column reaches the
`VectorAssembler`. This is unit-tested in `tests/test_features.py`.

| Column(s) | Model 1 (ARR_DELAY) | Model 2 (cause) | Reason |
|---|---|---|---|
| `ARR_DELAY`, `ARR_DELAY_NEW`, `ARR_DEL15`, `DELAY_CATEGORY` | target / **exclude** | **exclude** | The target itself, or derived from it after landing. For Model 2, `ARR_DELAY ≥ 15` only *defines the population*. |
| The 5 cause columns | **exclude** | **exclude** | Model 2's label is computed from them. For Model 1 they sum to the target. |
| `ARR_TIME`, `AIR_TIME`, `TAXI_IN`, `ACTUAL_ELAPSED_TIME` | **exclude** | **exclude** | Known only after landing |
| `DEP_DELAY`, `DEP_DEL15`, `TAXI_OUT`, `DEP_TIME` | `atdep` only | `atdep` only | Known at departure, not at scheduling |
| `CANCELLED`, `DIVERTED` | filter | filter | These flights have no arrival delay. They are removed and counted, never used as features. |
| Carrier, `ORIGIN`, `DEST`, `DISTANCE`, CRS times → `DEP_HOUR` / `ARR_HOUR`, `MONTH`, `DAY_OF_WEEK`, `DEP_TIME_CATEGORY`, `DISTANCE_CATEGORY` | keep | keep | Known when the schedule is published |

## 4. Engineered features

All historical features are **computed on the training months only** and then joined onto
both splits. Each is a smoothed mean, `(sum + m·global) / (count + m)` with m = 50, so
rarely seen keys shrink towards the global value. Keys never seen in training get the global value.

- **Model 1:** historical mean arrival delay per route, carrier, origin, destination and
  (origin, departure hour). Historical late rate per route and per carrier.
- **Model 2:** historical share of each of the 5 dominant causes per carrier, per origin and per
  route, giving 15 features. This captures "this carrier's delays are mostly carrier-caused" and
  "this airport's delays are mostly weather".
- **Both:** `ORIGIN_HOUR_DEPARTURES` counts scheduled departures from the same airport in the
  same hour on the same day, as a congestion proxy. It is built from the timetable only.
- **Categoricals:** `StringIndexer(handleInvalid="keep")`, plus `OneHotEncoder` for the linear
  models. The trees use the indexed value directly, with `maxBins` sized to the largest
  cardinality. `ROUTE` (thousands of levels) enters only through its encodings.
- **Missing numeric values:** imputed with the *training* median. Counts are reported in `feature_report.json`.

## 5. Split and training details

- **Temporal split.** Train on January–September, test on October–December, set with
  `--train-months/--test-months` in `features.py`. A random split would put flights from the
  same day in both sets, and delays propagate through the day. The temporal split also matches
  real use: trained on history, applied to the future. *Caveat:* the test months include
  winter, so the cause mix shifts towards weather. The report should say so.
- **Model 1 label winsorising.** The training label is clipped at the training 0.1% and 99.9%
  quantiles, so a handful of 20-hour delays cannot dominate the squared loss. **Evaluation always
  uses the raw test label.**
- **Model 2 class weights.** SECURITY_DELAY is extremely rare. Each class gets an
  inverse-frequency weight, `n / (k · n_class)`, computed on train and capped at 50.
  Without weights the model would rarely predict the minority classes. Side effect to watch:
  with a weight of up to 50 on a few hundred SECURITY rows, the forest can spend splits on the
  `HIST_*_SHARE_SECURITY` features, which then rank high in feature importance. Read
  importance together with the per-class F1 before claiming a feature "matters".
- **Random-forest regression on the full data** uses a sampled 30% of training rows by default
  (`RF_FRACTION_REGRESSION`), to fit in laptop memory. The fraction actually used is written to
  `outputs/ml/train_log.json` and must be quoted with the results.

## 6. Evaluation (outputs/ml/metrics.json)

**Model 1:** RMSE, MAE, median absolute error and R² on the test months. These are compared with
two constant baselines, the train mean and the train median. Because the delay distribution is
heavily right-skewed, the median baseline is hard to beat on MAE. MAE is also broken down by
actual-delay band (< 15, 15–60, 60–180, 180+). Expect the pre-departure model to badly
under-predict the large delays: they are driven by weather and knock-on effects that the
timetable cannot know.

**Model 2:**
- accuracy, compared with the majority-class baseline
- **macro-F1**, the headline metric because classes are imbalanced
- weighted-F1
- per-class precision, recall and F1
- the 5×5 confusion matrix with absolute counts
- train and test class distributions

Near-zero recall on SECURITY_DELAY is expected and should be reported, not hidden.

## 7. What to say about the numbers

- Report what the model actually achieves. A modest pre-departure model is an honest and expected result.
- The `pre` → `atdep` jump on Model 1 shows that arrival delay ≈ departure delay + noise.
  The useful question is how much the schedule alone explains, and the answer is not much.
- For Model 2, the confusion between NAS and late-aircraft delay is the interesting failure.
  Both are congestion phenomena that build up through the day.
