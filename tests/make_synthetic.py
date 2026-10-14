"""Generate data/samples/synthetic_smoke.csv — a SYNTHETIC fixture for pipeline smoke tests.

It mimics the shape of the cleaned BTS file (same column names, quoted city names with commas,
empty cause columns for on-time flights, cancelled / diverted rows, a few malformed lines,
occasional exact cause ties) so every stage can be exercised without the real data.

The numbers it produces mean NOTHING about real flights. Every output derived from it is
stamped mode=synthetic and every chart carries a SYNTHETIC footer.
"""
import csv
import datetime as dt
import math
import os
import random
import sys

ROOT = os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(ROOT, "data", "samples", "synthetic_smoke.csv")

HEADER = [
    "YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK", "FL_DATE", "OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM",
    "ORIGIN", "ORIGIN_CITY_NAME", "DEST", "DEST_CITY_NAME", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY",
    "DEP_DEL15", "TAXI_OUT", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "ARR_DEL15", "CANCELLED",
    "DIVERTED", "AIR_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY", "ROUTE", "DEP_TIME_CATEGORY", "DELAY_CATEGORY", "DISTANCE_CATEGORY",
]

AIRPORTS = {  # code: (city, weather susceptibility, congestion)
    "ATL": ("Atlanta, GA", 0.8, 1.6), "ORD": ("Chicago, IL", 1.6, 1.5), "DFW": ("Dallas/Fort Worth, TX", 1.1, 1.4),
    "DEN": ("Denver, CO", 1.5, 1.3), "LAX": ("Los Angeles, CA", 0.3, 1.3), "JFK": ("New York, NY", 1.2, 1.5),
    "SFO": ("San Francisco, CA", 0.9, 1.2), "SEA": ("Seattle, WA", 0.8, 1.0), "MIA": ("Miami, FL", 1.0, 1.1),
    "BOS": ("Boston, MA", 1.4, 1.1), "PHX": ("Phoenix, AZ", 0.4, 1.0), "LAS": ("Las Vegas, NV", 0.3, 1.1),
    "MSP": ("Minneapolis, MN", 1.5, 1.0), "DTW": ("Detroit, MI", 1.3, 1.0), "CLT": ("Charlotte, NC", 0.8, 1.2),
    "EWR": ("Newark, NJ", 1.2, 1.6), "IAH": ("Houston, TX", 1.0, 1.1), "MCO": ("Orlando, FL", 1.1, 1.2),
    "SLC": ("Salt Lake City, UT", 0.9, 0.9), "BNA": ("Nashville, TN", 0.8, 0.9),
}
CARRIERS = {"AA": 1.1, "DL": 0.7, "UA": 1.0, "WN": 1.2, "B6": 1.5, "AS": 0.8, "NK": 1.4, "F9": 1.6}


def hhmm(minutes):
    minutes %= 24 * 60
    return f"{minutes // 60}{minutes % 60:02d}"


def category(hour):
    return "Morning" if 5 <= hour < 12 else "Afternoon" if hour < 17 else "Evening" if hour < 21 else "Night"


def main(n=20000, seed=7):
    rng = random.Random(seed)
    codes = list(AIRPORTS)
    routes = []
    while len(routes) < 120:
        o, d = rng.sample(codes, 2)
        if (o, d) not in routes:
            routes.append((o, d))
    dist = {r: rng.randint(250, 2600) for r in routes}
    route_carriers = {r: rng.sample(list(CARRIERS), 3) for r in routes}

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    start = dt.date(2025, 1, 1)
    with open(OUT, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        for i in range(n):
            day = start + dt.timedelta(days=rng.randrange(365))
            o, d = rng.choice(routes)
            car = rng.choice(route_carriers[(o, d)])
            hour = min(23, max(5, int(rng.gauss(13, 4.5))))
            dep_min = hour * 60 + rng.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
            distance = dist[(o, d)]
            block = int(distance / 8 + 35)
            winter = 1.0 + 1.2 * math.cos(2 * math.pi * (day.month - 1) / 12) ** 2 * (day.month in (12, 1, 2))
            summer = 1.0 + 0.8 * (day.month in (6, 7, 8))
            wx_sus, cong = AIRPORTS[o][1], AIRPORTS[o][2]

            # Latent delay components (minutes); most are zero for most flights.
            carrier = rng.expovariate(1 / (6 * CARRIERS[car])) if rng.random() < 0.25 else 0
            weather = rng.expovariate(1 / 40) if rng.random() < 0.03 * wx_sus * winter * summer else 0
            nas = rng.expovariate(1 / (8 * cong)) if rng.random() < 0.18 * cong else 0
            late_ac = rng.expovariate(1 / 20) if rng.random() < 0.02 + 0.014 * max(0, hour - 6) else 0
            security = rng.expovariate(1 / 10) if rng.random() < 0.003 else 0

            cancelled = rng.random() < 0.012 * winter * (1 + weather / 60)
            diverted = not cancelled and rng.random() < 0.003
            dep_delay = round(carrier + weather + late_ac + security + nas * 0.3 + rng.gauss(-4, 4))
            taxi_out = max(5, round(rng.gauss(14 + 6 * (cong - 1), 4) + nas * 0.4))
            taxi_in = max(3, round(rng.gauss(7, 2)))
            arr_delay = round(dep_delay + nas * 0.7 + (taxi_out - 15) + rng.gauss(-3, 6))

            row = {
                "YEAR": 2025, "MONTH": day.month, "DAY_OF_MONTH": day.day, "DAY_OF_WEEK": day.isoweekday(),
                "FL_DATE": day.isoformat(), "OP_UNIQUE_CARRIER": car, "OP_CARRIER_FL_NUM": 100 + i % 4000,
                "ORIGIN": o, "ORIGIN_CITY_NAME": AIRPORTS[o][0], "DEST": d, "DEST_CITY_NAME": AIRPORTS[d][0],
                "CRS_DEP_TIME": hhmm(dep_min), "CRS_ARR_TIME": hhmm(dep_min + block),
                "CANCELLED": "1.0" if cancelled else "0.0", "DIVERTED": "1.0" if diverted else "0.0",
                "DISTANCE": f"{distance}.0", "ROUTE": f"{o}_{d}", "DEP_TIME_CATEGORY": category(hour),
                "DISTANCE_CATEGORY": "Short" if distance < 700 else "Medium" if distance < 1500 else "Long",
            }
            if cancelled or diverted:
                row["DELAY_CATEGORY"] = "No_Operation"
                if diverted:
                    row.update(DEP_TIME=hhmm(dep_min + dep_delay), DEP_DELAY=f"{dep_delay}.0", TAXI_OUT=taxi_out)
            else:
                row.update(
                    DEP_TIME=hhmm(dep_min + dep_delay), DEP_DELAY=f"{dep_delay}.0",
                    DEP_DEL15="1.0" if dep_delay >= 15 else "0.0", TAXI_OUT=f"{taxi_out}.0",
                    TAXI_IN=f"{taxi_in}.0", ARR_TIME=hhmm(dep_min + block + arr_delay),
                    ARR_DELAY=f"{arr_delay}.0", ARR_DEL15="1.0" if arr_delay >= 15 else "0.0",
                    AIR_TIME=f"{block - taxi_out - taxi_in}.0",
                    DELAY_CATEGORY="On_Time" if arr_delay < 15 else "Minor_Delay" if arr_delay < 60
                    else "Moderate_Delay" if arr_delay < 180 else "Severe_Delay",
                )
                if arr_delay >= 15:  # BTS reports causes only for late arrivals, summing to ARR_DELAY
                    parts = [carrier, weather, nas, security, late_ac]
                    if sum(parts) <= 0:
                        parts = [0, 0, 1, 0, 0]
                    if rng.random() < 0.01:  # an exact tie between two causes
                        a, b = rng.sample(range(5), 2)
                        parts = [0] * 5
                        parts[a] = parts[b] = 1
                    total = sum(parts)
                    mins = [int(arr_delay * p / total) for p in parts]
                    mins[max(range(5), key=lambda k: parts[k])] += arr_delay - sum(mins)
                    for name, v in zip(HEADER[25:30], mins):
                        row[name] = f"{v}.0"
            w.writerow([row.get(h, "") for h in HEADER])

        # A few malformed lines, which ingest must count and drop.
        for _ in range(5):
            w.writerow(["2025", "", "", "", "not-a-date", "AA", "1", "ATL", "Atlanta, GA", "ORD"] + [""] * 24)
    print(f"wrote {OUT}: {n} synthetic rows + 5 malformed (seed {seed}) — NOT real data")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 20000)
