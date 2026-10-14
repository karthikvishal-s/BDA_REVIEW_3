"""OPTIONAL dashboard: serves the precomputed outputs only. No Spark, no model scoring here.

    make api   ->  http://localhost:8000
"""
import json
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUTS = os.path.join(ROOT, "outputs")

app = FastAPI(title="Flight Delay — Review 3")
app.mount("/plots", StaticFiles(directory=os.path.join(OUTPUTS, "plots")), name="plots")


def _read(*parts):
    p = os.path.join(OUTPUTS, *parts)
    if not os.path.exists(p):
        raise HTTPException(404, f"{'/'.join(parts)} not generated yet — run the pipeline first")
    with open(p) as fh:
        return json.load(fh)


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.get("/api/plots")
def plots():
    d = os.path.join(OUTPUTS, "plots")
    return sorted(f for f in os.listdir(d) if f.endswith(".png"))


@app.get("/api/metrics")
def metrics():
    return _read("ml", "metrics.json")


@app.get("/api/predictions")
def predictions():
    return _read("ml", "predict_demo.json")


@app.get("/api/scala/{name}")
def scala(name: str):
    if "/" in name or not name.endswith(".json"):
        raise HTTPException(400, "expected a file name such as route_stats.json")
    return _read("scala", name)
