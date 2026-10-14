# Demo script (≈ 12 minutes + live question)

Prepare beforehand: `make all INPUT=full` finished, and `make history` running in a second
terminal. Have `outputs/plots/` and `docs/ML_DESIGN.md` open in tabs.

| # | Who | Time | Action | Talking point |
|---|---|---|---|---|
| 1 | Sathya | 1 min | Architecture slide | Reviews 1 → 2 → 3: Hive batch, HBase lookups, now Spark in-memory processing and ML on the same 7,001,620 flights |
| 2 | Darun | 1.5 min | `cat outputs/logs/ingest.txt` | Rows read, malformed, cancelled and diverted; Parquet reconciled with the CSV; partitioned by MONTH |
| 3 | Karthik | 3 min | `make rdd INPUT=sample HOLD=300` live (~1 min), then walk through `outputs/logs/rdd_demo.txt` from the full run | map/filter/flatMap/reduceByKey/mapValues, the actions, aggregateByKey and join |
| 4 | Karthik | 1.5 min | Spark UI :4040 → Jobs → DAG of the reduceByKey job; `cat outputs/logs/lineage.txt` | Narrow vs wide: the stage boundary is the ShuffledRDD; fault tolerance is recompute-from-lineage |
| 5 | Darun | 1 min | `cat outputs/logs/partitions.txt outputs/logs/timings.txt` | Partition counts, and the cache experiment with the numbers as observed |
| 6 | Abhinav | 2.5 min | `docs/ML_DESIGN.md` §1–3, then charts 06–08 | Two problems, two feature sets, the leakage table; why `pre` is low and `atdep` high; macro-F1 against the majority baseline |
| 7 | Abhinav | 0.5 min | `make predict INPUT=full` | Predicted minutes, then predicted cause, next to the actual values |
| 8 | Sathya | 1 min | Charts 01–03, 09 | Worst routes, seasonality and time of day, cause mix, what the forest relies on |

## Live analysis question
Pick the closest template and run it against the full Parquet. Each takes seconds to tens of seconds:
```bash
make query INPUT=full Q='top-routes --n 10 --month 7'
make query INPUT=full Q='carrier-delays --carrier AA'
make query INPUT=full Q='busiest-hour --airport ATL'
make query INPUT=full Q='compare-carriers --route ATL_LGA --carriers DL AA'
make query INPUT=full Q='cancellations --month 1'
make query INPUT=full Q='cause-mix --airport ORD'
make query INPUT=full Q='sql --q "SELECT MONTH, avg(ARR_DELAY) d FROM flights GROUP BY MONTH ORDER BY MONTH"'
```
Rehearse each one and write its timing here: ______
