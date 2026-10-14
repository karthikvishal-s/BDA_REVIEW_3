"""Render every chart from the real pipeline outputs (outputs/scala, outputs/ml) to outputs/plots/.

No number is typed by hand: each chart reads a JSON file and carries a footer naming that file,
its input mode and row count. Outputs from a sample or synthetic run get a loud banner.
A chart whose source file is missing is skipped with a message, never faked.
"""
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

ROOT = os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(ROOT, "outputs", "plots")

# Reference palette (dataviz skill, light mode). Causes keep the same slot in every chart.
SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
NEUTRAL = "#a3a29c"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
CAUSES = ["CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"]
CAUSE_NAME = {"CARRIER_DELAY": "Carrier", "WEATHER_DELAY": "Weather", "NAS_DELAY": "NAS (air traffic)",
              "SECURITY_DELAY": "Security", "LATE_AIRCRAFT_DELAY": "Late aircraft"}
CAUSE_COLOR = dict(zip(CAUSES, SERIES))
FS_COLOR = {"baseline": NEUTRAL, "pre": SERIES[0], "atdep": SERIES[1]}
FS_NAME = {"pre": "Pre-departure", "atdep": "At-departure"}
BLUES = LinearSegmentedColormap.from_list("seq", ["#f4f8fe", "#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])
MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
DAYS = "Mon Tue Wed Thu Fri Sat Sun".split()

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "text.color": INK,
    "xtick.color": INK_2, "ytick.color": INK_2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 10, "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.titlepad": 12, "legend.frameon": False,
})


def load(rel):
    p = os.path.join(ROOT, rel)
    if not os.path.exists(p):
        print(f"skip: {rel} not found")
        return None
    with open(p) as fh:
        return json.load(fh)


def save(fig, name, src, data):
    m = data.get("_meta", {})
    mode = m.get("mode", "?")
    foot = f"Source: {src}  ·  input={m.get('input_path', '?')}  ·  rows={m.get('row_count', 0):,}  ·  {m.get('generated_at', '')}"
    h = fig.get_figheight()
    line = 0.16 / h  # one footer line, in figure fraction, independent of figure height
    fig.text(0.01, 0.25 * line, foot, fontsize=7, color=INK_2, ha="left", va="bottom")
    bottom = 1.4 * line
    if mode != "full":
        banner = "SYNTHETIC DATA — pipeline check only, not real results" if mode == "synthetic" \
            else "SAMPLE (100k rows) — not full-dataset results"
        fig.text(0.01, 1.35 * line, banner, fontsize=8, color="#e34948", ha="left", va="bottom", weight="bold")
        bottom = 2.6 * line
    fig.tight_layout(rect=(0, bottom, 1, 1))
    os.makedirs(OUT, exist_ok=True)
    fig.savefig(os.path.join(OUT, name), dpi=200)
    plt.close(fig)
    print(f"wrote outputs/plots/{name}")


def hbar(ax, labels, values, color, fmt="{:.1f}"):
    y = np.arange(len(labels))[::-1]
    ax.barh(y, values, color=color, height=0.7, edgecolor=SURFACE, linewidth=1)
    ax.set_yticks(y, labels)
    ax.grid(axis="y", visible=False)
    for yi, v in zip(y, values):
        ax.text(max(v, 0), yi, " " + fmt.format(v), va="center", fontsize=8, color=INK_2)
    if min(values) < 0:
        ax.axvline(0, color=INK_2, lw=0.8)


# --------------------------------------------------------------------------- 1. routes
def routes():
    d = load("outputs/scala/route_stats.json")
    if not d or not d["by_avg_delay"]:
        return
    rows = d["by_avg_delay"][:15]
    fig, ax = plt.subplots(figsize=(9, 6))
    hbar(ax, [f"{r['route'].replace('_', ' → ')}  ({r['flights']:,})" for r in rows],
         [r["avg_arr_delay"] for r in rows], SERIES[0])
    ax.set_xlabel("Average arrival delay (minutes)")
    ax.set_title(f"Top 15 routes by average arrival delay (routes with ≥ {d['min_flights']} flights)")
    save(fig, "01_top_routes.png", "outputs/scala/route_stats.json", d)


# --------------------------------------------------------------------------- 2. temporal
def temporal():
    d = load("outputs/scala/temporal.json")
    if not d:
        return
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5), gridspec_kw={"width_ratios": [12, 7, 18]})
    for ax, key, k, labels, title in [
        (axes[0], "by_month", "month", MONTHS, "By month"),
        (axes[1], "by_day_of_week", "day_of_week", DAYS, "By day of week"),
        (axes[2], "by_dep_hour", "dep_hour", None, "By scheduled departure hour"),
    ]:
        rows = d[key]
        x = [r[k] for r in rows]
        v = [100 * r["delay_rate"] for r in rows]
        ax.bar(x, v, color=SERIES[0], width=0.75, edgecolor=SURFACE, linewidth=1)
        if labels:
            ax.set_xticks(x, [labels[i - 1] for i in x])
        else:
            ax.set_xticks([h for h in x if h % 2 == 0])
        ax.set_title(title, fontsize=11)
        ax.grid(axis="x", visible=False)
        i = int(np.argmax(v))
        ax.annotate(f"{v[i]:.1f}%", (x[i], v[i]), ha="center", va="bottom", fontsize=8, color=INK_2,
                    xytext=(0, 2), textcoords="offset points")
    axes[0].set_ylabel("Flights arriving ≥ 15 min late (%)")
    fig.suptitle("When delays happen — share of operated flights arriving 15+ minutes late",
                 x=0.01, ha="left", fontweight="bold", fontsize=13)
    save(fig, "02_temporal_delay_rate.png", "outputs/scala/temporal.json", d)


# --------------------------------------------------------------------------- 3. causes
def causes():
    d = load("outputs/scala/causes.json")
    if not d:
        return
    by = {c["cause"]: c for c in d["causes"]}
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.8))
    names = [CAUSE_NAME[c] for c in CAUSES]
    colors = [CAUSE_COLOR[c] for c in CAUSES]
    hbar(a1, names, [100 * by[c]["minutes_share"] for c in CAUSES], colors, "{:.1f}%")
    a1.set_xlabel("Share of all attributed delay minutes (%)")
    a1.set_title("Delay minutes by cause", fontsize=11)
    dom_total = sum(by[c]["dominant_count"] for c in CAUSES) or 1
    hbar(a2, names, [100 * by[c]["dominant_count"] / dom_total for c in CAUSES], colors, "{:.1f}%")
    a2.set_xlabel("Late flights where this cause is the largest (%)")
    a2.set_title("Dominant cause per late flight (Model 2 label)", fontsize=11)
    fig.suptitle(f"What causes delays — {d['delayed_flights']:,} late flights with reported causes",
                 x=0.01, ha="left", fontweight="bold", fontsize=13)
    save(fig, "03_delay_causes.png", "outputs/scala/causes.json", d)


# --------------------------------------------------------------------------- 4. regression residuals
def regression_scatter():
    d = load("outputs/ml/regression_residuals.json")
    if not d:
        return
    sets = [fs for fs in ("pre", "atdep") if fs in d]
    if not sets:
        return
    fig, axes = plt.subplots(1, len(sets), figsize=(6.2 * len(sets), 5.4), squeeze=False)
    for ax, fs in zip(axes[0], sets):
        pts = np.array(d[fs]["actual_predicted"], dtype=float)
        a, p = np.clip(pts[:, 0], -60, 300), np.clip(pts[:, 1], -60, 300)
        hb = ax.hexbin(a, p, gridsize=40, cmap=BLUES, mincnt=1, bins="log", linewidths=0,
                       extent=(-60, 300, -60, 300))
        ax.plot([-60, 300], [-60, 300], color=INK_2, lw=1, ls="--")
        ax.text(250, 262, "perfect", color=INK_2, fontsize=8, rotation=45)
        ax.set_xlim(-60, 300); ax.set_ylim(-60, 300)
        ax.set_xlabel("Actual arrival delay (min, clipped −60…300)")
        ax.set_ylabel("Predicted arrival delay (min)")
        ax.set_title(f"{FS_NAME[fs]} — {d[fs]['model'].split('_')[-1].upper()}  (n={len(pts):,} sampled)",
                     fontsize=11)
        fig.colorbar(hb, ax=ax, label="flights (log)")
    fig.suptitle("Model 1 — predicted vs actual arrival delay on the test months",
                 x=0.01, ha="left", fontweight="bold", fontsize=13)
    save(fig, "04_regression_pred_vs_actual.png", "outputs/ml/regression_residuals.json", d)

    fig, axes = plt.subplots(1, len(sets), figsize=(6.2 * len(sets), 4), squeeze=False, sharey=True)
    for ax, fs in zip(axes[0], sets):
        pts = np.array(d[fs]["actual_predicted"], dtype=float)
        res = np.clip(pts[:, 1] - pts[:, 0], -150, 150)
        ax.hist(res, bins=60, color=FS_COLOR[fs], edgecolor=SURFACE, linewidth=0.5)
        ax.axvline(0, color=INK_2, lw=1, ls="--")
        ax.set_xlabel("Residual = predicted − actual (min, clipped ±150)")
        ax.set_title(FS_NAME[fs], fontsize=11)
    axes[0][0].set_ylabel("Flights")
    fig.suptitle("Model 1 — residual distribution (long right tail = under-predicted big delays)",
                 x=0.01, ha="left", fontweight="bold", fontsize=13)
    save(fig, "05_regression_residuals.png", "outputs/ml/regression_residuals.json", d)


# --------------------------------------------------------------------------- 5/6/7. metrics
def regression_bars(metrics):
    r = metrics.get("regression")
    if not r:
        return
    rows = [("Baseline: train median", "baseline", r["baselines"]["train_median"]),
            ("Baseline: train mean", "baseline", r["baselines"]["train_mean"])]
    for fs in ("pre", "atdep"):
        for algo, m in r["models"].get(fs, {}).items():
            rows.append((f"{FS_NAME[fs]} · {algo.upper()}", fs, m))
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.6), sharey=True)
    for ax, key, label, fmt in [(axes[0], "mae", "MAE (minutes) — lower is better", "{:.1f}"),
                                (axes[1], "rmse", "RMSE (minutes) — lower is better", "{:.1f}"),
                                (axes[2], "r2", "R² — higher is better", "{:.3f}")]:
        hbar(ax, [x[0] for x in rows], [x[2][key] or 0 for x in rows], [FS_COLOR[x[1]] for x in rows], fmt)
        ax.set_xlabel(label)
    fig.suptitle(f"Model 1 — arrival-delay regression on {r['test_rows']:,} test flights",
                 x=0.01, ha="left", fontweight="bold", fontsize=13)
    save(fig, "06_regression_metrics.png", "outputs/ml/metrics.json", metrics)


def best_cause_model(c, fs):
    models = c["models"].get(fs, {})
    if not models:
        return None, None
    algo = max(models, key=lambda a: models[a]["macro_f1"])
    return algo, models[algo]


def confusion(metrics):
    c = metrics.get("cause")
    if not c:
        return
    sets = [fs for fs in ("pre", "atdep") if best_cause_model(c, fs)[0]]
    fig, axes = plt.subplots(1, len(sets), figsize=(7 * len(sets), 6), squeeze=False)
    names = [CAUSE_NAME[x] for x in CAUSES]
    for ax, fs in zip(axes[0], sets):
        algo, m = best_cause_model(c, fs)
        cm = np.array(m["confusion_matrix"]["rows_actual_cols_predicted"], dtype=float)
        norm = cm / np.maximum(cm.sum(axis=1, keepdims=True), 1)
        ax.imshow(norm, cmap=BLUES, vmin=0, vmax=1)
        ax.grid(False)
        for i in range(5):
            for j in range(5):
                ax.text(j, i, f"{100 * norm[i, j]:.0f}%\n{int(cm[i, j]):,}", ha="center", va="center",
                        fontsize=8, color="white" if norm[i, j] > 0.55 else INK)
        ax.set_xticks(range(5), names, rotation=30, ha="right")
        ax.set_yticks(range(5), names)
        ax.set_xlabel("Predicted cause"); ax.set_ylabel("Actual dominant cause")
        ax.set_title(f"{FS_NAME[fs]} · {algo.upper()}   acc {m['accuracy']:.2f} · macro-F1 {m['macro_f1']:.2f}",
                     fontsize=11)
    fig.suptitle("Model 2 — dominant-cause confusion matrix (row-normalised: % = recall per actual cause)",
                 x=0.01, ha="left", fontweight="bold", fontsize=13)
    save(fig, "07_cause_confusion_matrix.png", "outputs/ml/metrics.json", metrics)


def per_class_f1(metrics):
    c = metrics.get("cause")
    if not c:
        return
    series = [("Majority baseline", "baseline", c["baselines"]["majority_class"])]
    for fs in ("pre", "atdep"):
        algo, m = best_cause_model(c, fs)
        if m:
            series.append((f"{FS_NAME[fs]} · {algo.upper()}", fs, m))
    x = np.arange(len(CAUSES) + 1)
    w = 0.8 / len(series)
    fig, ax = plt.subplots(figsize=(12, 4.8))
    for k, (label, fs, m) in enumerate(series):
        vals = [m["per_class"][cz]["f1"] for cz in CAUSES] + [m["macro_f1"]]
        bars = ax.bar(x - 0.4 + w * (k + 0.5), vals, w, color=FS_COLOR[fs], label=label,
                      edgecolor=SURFACE, linewidth=1)
        ax.bar_label(bars, [f"{v:.2f}" for v in vals], fontsize=7, color=INK_2, padding=2)
    ax.set_xticks(x, [CAUSE_NAME[cz] for cz in CAUSES] + ["Macro-F1"])
    ax.set_ylabel("F1 score"); ax.set_ylim(0, 1.05)
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper right", ncol=len(series))
    ax.set_title("Model 2 — per-class F1 on the test months (support: "
                 + ", ".join(f"{CAUSE_NAME[cz]} {c['class_distribution_test'].get(cz, 0):,}" for cz in CAUSES) + ")",
                 fontsize=10)
    save(fig, "08_cause_per_class_f1.png", "outputs/ml/metrics.json", metrics)


# --------------------------------------------------------------------------- 8. importance
def importances():
    for num, task, title in (("09", "regression", "Model 1 — arrival delay"),
                             ("10", "cause", "Model 2 — dominant cause")):
        d = load(f"outputs/ml/{task}_pre_rf_importance.json")
        if not d:
            continue
        top = d["top"][:15]
        fig, ax = plt.subplots(figsize=(10, 5.8))
        hbar(ax, [t["feature"] for t in top], [t["importance"] for t in top], SERIES[0], "{:.3f}")
        ax.set_xlabel("Random-forest feature importance (mean impurity decrease, sums to 1)")
        fig.suptitle(f"{title}: what the pre-departure random forest relies on",
                     x=0.01, ha="left", fontweight="bold", fontsize=13)
        save(fig, f"{num}_{task}_pre_importance.png", f"outputs/ml/{task}_pre_rf_importance.json", d)


def main():
    routes()
    temporal()
    causes()
    regression_scatter()
    metrics = load("outputs/ml/metrics.json")
    if metrics:
        regression_bars(metrics)
        confusion(metrics)
        per_class_f1(metrics)
    importances()


if __name__ == "__main__":
    main()
