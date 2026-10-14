"""Paths, provenance stamping and Spark session shared by every PySpark stage.

INPUT=full|sample|synthetic (environment, or --input) selects the data source. Every JSON
written through write_json() carries a _meta block saying which input produced it, so a
sample or synthetic result can never be mistaken for a full-dataset result.
"""
import argparse
import datetime as dt
import json
import os

ROOT = os.environ.get("PROJECT_ROOT", "/work")

RAW = {
    "full": "data/raw/flight_data_cleaned_1year.csv",
    "sample": "data/samples/sample_100k.csv",
    "synthetic": "data/samples/synthetic_smoke.csv",
}

CAUSES = ["CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"]

# Temporal split (doc §5.5): earlier months train, later months test.
TRAIN_MONTHS = list(range(1, 10))
TEST_MONTHS = [10, 11, 12]


def path(*parts):
    return os.path.join(ROOT, *parts)


def processed(mode):
    return path("data", "processed", mode, "flights.parquet")


def features_dir(mode):
    return path("data", "features", mode)


def models_dir(mode):
    return path("models", mode)


def parser(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--input", default=os.environ.get("INPUT", "sample"), choices=sorted(RAW))
    return p


def spark_session(app, mode):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"{app} [{mode}]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def meta(mode, row_count, producer):
    return {
        "input_path": RAW[mode],
        "mode": mode,
        "row_count": int(row_count),
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "producer": producer,
    }


def write_json(rel_path, payload, mode, row_count, producer):
    full = path(rel_path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w") as fh:
        json.dump({"_meta": meta(mode, row_count, producer), **payload}, fh, indent=2, default=_jsonable)
        fh.write("\n")
    print(f"wrote {rel_path}")
    return full


def read_json(rel_path):
    with open(path(rel_path)) as fh:
        return json.load(fh)


def _jsonable(x):
    if hasattr(x, "item"):  # numpy scalars
        return x.item()
    if isinstance(x, (dt.date, dt.datetime)):
        return x.isoformat()
    raise TypeError(f"not JSON serialisable: {type(x)}")


def require_processed(mode):
    if not os.path.isdir(processed(mode)):
        raise SystemExit(f"{processed(mode)} not found — run `make ingest INPUT={mode}` first.")
