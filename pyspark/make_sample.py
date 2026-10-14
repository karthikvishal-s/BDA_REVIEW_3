"""Write data/samples/sample_100k.csv: 100,000 lines drawn at random from the REAL CSV.

The original lines are copied byte for byte (no re-quoting), with the header first, so the
sample exercises exactly the same parsing path as the full file. Seeded, so it is reproducible.
"""
import os
import random

from common import RAW, path, spark_session

N = 100_000
SEED = 42


def main():
    src = path(RAW["full"])
    if not os.path.exists(src):
        raise SystemExit(f"{src} not found — copy flight_data_cleaned_1year.csv into data/raw/ first.")
    spark = spark_session("MakeSample", "full")
    lines = spark.sparkContext.textFile(src)
    header = lines.first()
    body = lines.filter(lambda l: l != header)
    total = body.count()
    # take(N) straight off the sample would favour early partitions (early months, if the file is
    # date-ordered), so collect the slightly larger uniform sample and subsample it in memory.
    pool = body.sample(False, min(1.0, 1.15 * N / total), SEED).collect()
    random.Random(SEED).shuffle(pool)
    picked = pool[:N]

    out = path(RAW["sample"])
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as fh:
        fh.write(header + "\n")
        fh.writelines(l + "\n" for l in picked)
    print(f"wrote {RAW['sample']}: {len(picked):,} of {total:,} lines (seed {SEED})")
    spark.stop()


if __name__ == "__main__":
    main()
