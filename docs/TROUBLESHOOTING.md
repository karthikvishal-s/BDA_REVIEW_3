# Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop is not running | Start it and wait for the whale icon to go steady |
| `Bind for 0.0.0.0:4040 failed: port is already allocated` | Another `make` target or a held RddDemo is still running | `docker ps`, then `docker stop <id>`; run one job at a time |
| `java.lang.OutOfMemoryError` / `GC overhead limit exceeded` / exit code 137 | Not enough memory for the 7M-row run | Raise Docker memory; lower `RF_TREES` / `RF_DEPTH`; set `RF_FRACTION_REGRESSION=0.2`; lower `spark.driver.memory` in `docker/spark-defaults.conf` if Docker has < 8 GB |
| `Required columns missing: …` from Ingest | The CSV header differs from the contract in `Common.scala` | Run `make schema-check`, then rename the column in the CSV or add it to `Common.Required` / `Optional` |
| `Empty split (train=…, test=…)` in features | The data does not cover months 1–12 | `docker compose … run spark spark-submit pyspark/features.py --train-months 1-8 --test-months 9-10`, and record the change |
| `… not found — run make ingest INPUT=…` | Stages run out of order, or for a different INPUT | Every stage needs the same INPUT as the ingest that preceded it |
| sbt is slow on the first `make` | It downloads Scala and Spark jars | Only happens once; the caches live in Docker volumes |
| sbt fails to compile | A Scala syntax error | Fallback for the demo: `make shell`, then `spark-shell`, then `:load scala/src/main/scala/Common.scala` and `:load …/RddDemo.scala`, then `RddDemo.main(Array())` |
| Spark UI is gone after the job finishes | The UI dies with the application | `make rdd HOLD=300` keeps it alive for 5 minutes, or `make history` → http://localhost:18080 |
| Charts show a red SAMPLE / SYNTHETIC banner | The outputs came from a non-full run | Re-run with `INPUT=full`. The banner exists so this can never be missed. |
| WSL2: everything is very slow | Repository is under `/mnt/c` | Clone into the Linux home directory |
