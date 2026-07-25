"""
Synthetic vehicle telemetry data generator.

Produces a CSV matching the schema required by the assignment brief:
vehicle_id, vehicle_model, timestamp, engine_temp, speed, battery_efficiency, lat, lon.

Deliberately injects severe key skew: 3 "hot" vehicle_ids (malfunctioning/high-frequency
sensors) contribute ~1000x the row volume of a typical vehicle. This skew is what the
PySpark salting step (Part 3.2) is designed to fix.

Reproducible via a fixed random seed.
"""

import numpy as np
import pandas as pd

RNG_SEED = 42
TOTAL_ROWS = 500_000
NUM_VEHICLES = 5_000
NUM_HOT_VEHICLES = 3
HOT_MULTIPLIER = 1000

VEHICLE_MODELS = [
    "Volvo_FH16",
    "Scania_R450",
    "MAN_TGX",
    "DAF_XF",
    "Mercedes_Actros",
    "Iveco_S-Way",
]

rng = np.random.default_rng(RNG_SEED)

# ---------------------------------------------------------------------------
# 1. Assign vehicle_ids to models (roughly even split across 6 models)
# ---------------------------------------------------------------------------
vehicle_ids = [f"VEH{str(i).zfill(6)}" for i in range(NUM_VEHICLES)]
vehicle_model_map = {
    vid: VEHICLE_MODELS[i % len(VEHICLE_MODELS)] for i, vid in enumerate(vehicle_ids)
}

# ---------------------------------------------------------------------------
# 2. Decide row count per vehicle_id: solve for baseline `b` such that
#    (NUM_VEHICLES - NUM_HOT_VEHICLES) * b + NUM_HOT_VEHICLES * (b * HOT_MULTIPLIER) = TOTAL_ROWS
# ---------------------------------------------------------------------------
normal_count = NUM_VEHICLES - NUM_HOT_VEHICLES
denominator = normal_count + NUM_HOT_VEHICLES * HOT_MULTIPLIER
baseline_rows_per_vehicle = round(TOTAL_ROWS / denominator)
hot_rows_per_vehicle = baseline_rows_per_vehicle * HOT_MULTIPLIER

hot_vehicle_ids = set(rng.choice(vehicle_ids, size=NUM_HOT_VEHICLES, replace=False))

row_counts = {}
for vid in vehicle_ids:
    row_counts[vid] = hot_rows_per_vehicle if vid in hot_vehicle_ids else baseline_rows_per_vehicle

actual_total = sum(row_counts.values())
# Spread any rounding remainder across normal vehicles (+/-1 row each) instead of
# dumping it all on one vehicle, which could otherwise drive a count negative.
remainder = TOTAL_ROWS - actual_total
normal_vids = [vid for vid in vehicle_ids if vid not in hot_vehicle_ids]
step = 1 if remainder > 0 else -1
for vid in normal_vids[: abs(remainder)]:
    row_counts[vid] += step

print(f"Baseline rows/vehicle: {baseline_rows_per_vehicle}")
print(f"Hot vehicle rows/vehicle: {hot_rows_per_vehicle}")
print(f"Hot vehicle_ids: {sorted(hot_vehicle_ids)}")
print(f"Sum of row_counts before remainder fix: {actual_total}")
print(f"Rounding remainder ({remainder}) spread across {min(abs(remainder), len(normal_vids))} normal vehicles")

# ---------------------------------------------------------------------------
# 3. Generate rows per vehicle
# ---------------------------------------------------------------------------
START_TS = pd.Timestamp("2024-01-01 00:00:00")
END_TS = pd.Timestamp("2024-01-31 23:59:59")
total_seconds_in_range = int((END_TS - START_TS).total_seconds())

chunks = []
for vid in vehicle_ids:
    n = row_counts[vid]
    model = vehicle_model_map[vid]
    is_hot = vid in hot_vehicle_ids

    # Timestamps: uniformly random offsets within the date range, then sorted.
    # Hot vehicles ping far more often within the same window -> denser timeline,
    # consistent with a "malfunctioning sensor spamming logs" narrative.
    offsets = rng.integers(0, total_seconds_in_range, size=n)
    timestamps = START_TS + pd.to_timedelta(offsets, unit="s")
    timestamps = np.sort(timestamps)

    # Engine temp: normal operating band 70-105C, ~2% chance of an overheat spike per row
    engine_temp = rng.normal(loc=88, scale=7, size=n)
    overheat_mask = rng.random(n) < 0.02
    engine_temp[overheat_mask] += rng.uniform(15, 30, size=overheat_mask.sum())
    engine_temp = np.clip(engine_temp, 60, 140)

    # Speed: highway logistics fleet, mostly 60-100 kph with idle/stop periods
    speed = np.clip(rng.normal(loc=75, scale=25, size=n), 0, 130)

    # Battery efficiency: slow degradation over time + noise
    battery_efficiency = np.clip(rng.normal(loc=88, scale=8, size=n), 30, 100)

    # GPS: global logistics fleet -> spread across plausible land-route latitudes/longitudes
    lat = rng.uniform(-55, 70, size=n)
    lon = rng.uniform(-180, 180, size=n)

    chunks.append(
        pd.DataFrame(
            {
                "vehicle_id": vid,
                "vehicle_model": model,
                "timestamp": timestamps,
                "engine_temp": np.round(engine_temp, 2),
                "speed": np.round(speed, 2),
                "battery_efficiency": np.round(battery_efficiency, 2),
                "lat": np.round(lat, 6),
                "lon": np.round(lon, 6),
            }
        )
    )

df = pd.concat(chunks, ignore_index=True)

# ---------------------------------------------------------------------------
# 3b. Inject invalid engine_temp readings (nulls + out-of-range sensor glitches)
#     so the downstream PySpark null/range filter (Part 3.1) has real invalid
#     rows to demonstrably remove, rather than filtering over already-clean data.
# ---------------------------------------------------------------------------
n_total = len(df)
null_idx = rng.choice(n_total, size=int(n_total * 0.005), replace=False)
remaining_idx = np.setdiff1d(np.arange(n_total), null_idx)
glitch_idx = rng.choice(remaining_idx, size=int(n_total * 0.005), replace=False)

df.loc[null_idx, "engine_temp"] = np.nan
# Sensor glitch values: clearly outside the physically plausible 60-140C range
df.loc[glitch_idx, "engine_temp"] = rng.choice([-999.0, 999.0], size=len(glitch_idx))

print(f"\nInjected {len(null_idx):,} null engine_temp readings")
print(f"Injected {len(glitch_idx):,} out-of-range engine_temp readings (sensor glitches)")

# Shuffle row order so timestamps aren't grouped by vehicle_id in the file
# (mirrors real streaming ingestion where pings from many vehicles interleave).
df = df.sample(frac=1, random_state=RNG_SEED).reset_index(drop=True)

OUTPUT_PATH = "synthetic_fleet_telemetry.csv"
df.to_csv(OUTPUT_PATH, index=False)

print(f"\nTotal rows written: {len(df):,}")
print(f"Output file: {OUTPUT_PATH}")
