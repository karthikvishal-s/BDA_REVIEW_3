"""Unit tests for the leakage discipline and the label definitions of both models."""
import pytest
from pyspark.sql import functions as F

from common import CAUSES
from features import (AT_DEPARTURE, CAUSE_ENCODINGS, POST_ARRIVAL, add_dominant_cause, assert_no_leakage,
                      attach, feature_columns, smoothed_means, temporal_split)
from evaluate import class_report


def causes_row(arr, carrier, weather, nas, security, late_ac):
    return (arr, carrier, weather, nas, security, late_ac)


def labelled(spark, rows):
    df = spark.createDataFrame(rows, ["ARR_DELAY"] + CAUSES)
    return {tuple(r[:6]): (r["DOMINANT_CAUSE"], r["CAUSE_STATUS"])
            for r in add_dominant_cause(df).collect()}


# --------------------------------------------------------------------------- Model 2 label
def test_dominant_cause_is_argmax(spark):
    out = labelled(spark, [
        causes_row(60.0, 10.0, 40.0, 10.0, 0.0, 0.0),
        causes_row(30.0, 0.0, 0.0, 5.0, 0.0, 25.0),
        causes_row(20.0, 0.0, 0.0, 0.0, 20.0, 0.0),
    ])
    assert out[(60.0, 10.0, 40.0, 10.0, 0.0, 0.0)] == ("WEATHER_DELAY", "ok")
    assert out[(30.0, 0.0, 0.0, 5.0, 0.0, 25.0)] == ("LATE_AIRCRAFT_DELAY", "ok")
    assert out[(20.0, 0.0, 0.0, 0.0, 20.0, 0.0)] == ("SECURITY_DELAY", "ok")


def test_ties_zero_sums_missing_and_on_time_get_no_label(spark):
    out = labelled(spark, [
        causes_row(40.0, 20.0, 20.0, 0.0, 0.0, 0.0),   # exact tie
        causes_row(25.0, 0.0, 0.0, 0.0, 0.0, 0.0),     # late but no cause minutes
        causes_row(25.0, None, None, None, None, None),  # causes not reported
        causes_row(5.0, None, None, None, None, None),  # on time
    ])
    assert out[(40.0, 20.0, 20.0, 0.0, 0.0, 0.0)] == (None, "tie")
    assert out[(25.0, 0.0, 0.0, 0.0, 0.0, 0.0)] == (None, "zero_causes")
    assert out[(25.0, None, None, None, None, None)] == (None, "causes_missing")
    assert out[(5.0, None, None, None, None, None)] == (None, "not_delayed")


# --------------------------------------------------------------------------- leakage guard
@pytest.mark.parametrize("col", sorted(POST_ARRIVAL))
def test_guard_rejects_post_arrival_columns(col):
    for fs in ("pre", "atdep"):
        with pytest.raises(ValueError):
            assert_no_leakage(["DISTANCE", col], "regression", fs)


@pytest.mark.parametrize("col", sorted(AT_DEPARTURE))
def test_guard_rejects_departure_columns_only_in_pre(col):
    with pytest.raises(ValueError):
        assert_no_leakage(["DISTANCE", col], "cause", "pre")
    assert_no_leakage(["DISTANCE", col], "cause", "atdep")


def test_feature_sets_differ_only_by_departure_columns():
    available = {"OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "MONTH", "DISTANCE", "DEP_HOUR", "DEP_DELAY", "TAXI_OUT"}
    for task in ("regression", "cause"):
        pre_c, pre_n = feature_columns(task, "pre", available)
        at_c, at_n = feature_columns(task, "atdep", available)
        assert pre_c == at_c
        assert set(at_n) - set(pre_n) == {"DEP_DELAY", "TAXI_OUT"}
    assert set(CAUSE_ENCODINGS) <= set(feature_columns("cause", "pre", available)[1])


# --------------------------------------------------------------------------- split and encodings
def test_temporal_split_has_no_month_overlap(spark):
    df = spark.createDataFrame([(m,) for m in range(1, 13)], ["MONTH"])
    train, test = temporal_split(df, range(1, 10), [10, 11, 12])
    assert {r.MONTH for r in train.collect()} == set(range(1, 10))
    assert {r.MONTH for r in test.collect()} == {10, 11, 12}
    with pytest.raises(ValueError):
        temporal_split(df, [1, 2, 10], [10, 11])


def test_encodings_use_training_rows_only(spark):
    train = spark.createDataFrame([("A_B", 10.0), ("A_B", 30.0), ("C_D", 100.0)], ["ROUTE", "ARR_DELAY"])
    test = spark.createDataFrame([("A_B", 999.0), ("Z_Z", 999.0)], ["ROUTE", "ARR_DELAY"])
    stats, g = smoothed_means(train, ["ROUTE"], {"ENC": F.col("ARR_DELAY")}, m=2)
    glob = (10 + 30 + 100) / 3
    assert g["ENC"] == pytest.approx(glob)

    enc = {r.ROUTE: r.ENC for r in attach(test, stats, ["ROUTE"], g).collect()}
    # Seen route: (sum + m*global) / (count + m) from TRAIN rows; the 999s in test never leak in.
    assert enc["A_B"] == pytest.approx((40 + 2 * glob) / (2 + 2))
    # Route never seen in training falls back to the training global mean.
    assert enc["Z_Z"] == pytest.approx(glob)


# --------------------------------------------------------------------------- evaluation maths
def test_class_report_on_known_confusion_matrix():
    m = {a: {p: 0 for p in CAUSES} for a in CAUSES}
    m["CARRIER_DELAY"]["CARRIER_DELAY"] = 8
    m["CARRIER_DELAY"]["NAS_DELAY"] = 2
    m["NAS_DELAY"]["NAS_DELAY"] = 6
    m["NAS_DELAY"]["CARRIER_DELAY"] = 4
    r = class_report(m)
    assert r["accuracy"] == pytest.approx(14 / 20)
    assert r["per_class"]["CARRIER_DELAY"]["precision"] == pytest.approx(8 / 12)
    assert r["per_class"]["CARRIER_DELAY"]["recall"] == pytest.approx(8 / 10)
    # Macro-F1 averages only classes present in the test data.
    f1c, f1n = r["per_class"]["CARRIER_DELAY"]["f1"], r["per_class"]["NAS_DELAY"]["f1"]
    assert r["macro_f1"] == pytest.approx((f1c + f1n) / 2)
