"""Generates the synthetic MDF4 fixture used by the test suite and the
example notebook.

    SYNTHETIC TEST DATA — NOT FROM A REAL VEHICLE

Every channel below is a deterministic, seeded function of time. There is no
real trip, ECU log, or customer data behind any of it — the numbers exist
only to give the test suite and the notebook something structurally
realistic (a short synthetic "drive cycle": accelerate, cruise, brake, idle)
to read, filter, and aggregate.

Run this script whenever the fixture needs to be regenerated:

    python src/test/resources/generate_fixture.py

The fixture contains only generic, synthetic channel names (no real vehicle,
project, or customer data) so it is safe to commit to the repository and to
a public GitHub history.
"""
import os

import numpy as np
from asammdf import MDF, Signal

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "data", "sample.mf4")

DURATION_S = 60.0

# A synthetic drive cycle expressed as (time_s, value) control points:
# 0-15s accelerate, 15-35s cruise, 35-50s brake to a stop, 50-60s idle.
_SPEED_CP = ((0.0, 0.0), (15.0, 100.0), (35.0, 100.0), (50.0, 0.0), (60.0, 0.0))
_ACCEL_CP = ((0.0, 0.0), (2.0, 90.0), (15.0, 35.0), (34.0, 35.0), (36.0, 0.0), (50.0, 0.0), (60.0, 8.0))
_BRAKE_CP = ((0.0, 0.0), (35.0, 0.0), (36.0, 55.0), (45.0, 20.0), (50.0, 0.0), (60.0, 0.0))
_BATTERY_CP = ((0.0, 11.6), (4.0, 12.4), (10.0, 12.7), (60.0, 12.7))


def _curve(timestamps: np.ndarray, control_points, noise_sigma: float, seed: int, lo: float, hi: float) -> np.ndarray:
    xp, fp = zip(*control_points)
    values = np.interp(timestamps, xp, fp)
    rng = np.random.default_rng(seed)
    values = values + rng.normal(0.0, noise_sigma, size=values.shape)
    return np.clip(values, lo, hi)


def _timestamps(step_s: float) -> np.ndarray:
    return np.arange(0.0, DURATION_S, step_s)


def build_channels() -> dict:
    """Build the synthetic (Signal, sample_step_s) pairs, one per channel.

    Sample rates deliberately differ across channels (as they typically do
    on a real measurement bus), which also exercises the connector reading
    channels from distinct MDF channel groups.
    """
    accel_ts = _timestamps(0.02)   # 50 Hz
    speed_ts = _timestamps(0.1)    # 10 Hz
    brake_ts = _timestamps(0.02)   # 50 Hz
    battery_ts = _timestamps(1.0)  # 1 Hz

    vehicle_speed = _curve(speed_ts, _SPEED_CP, noise_sigma=0.4, seed=1, lo=0.0, hi=180.0)
    accelerator_position = _curve(accel_ts, _ACCEL_CP, noise_sigma=1.5, seed=2, lo=0.0, hi=100.0)
    brake_pressure = _curve(brake_ts, _BRAKE_CP, noise_sigma=0.5, seed=3, lo=0.0, hi=120.0)
    battery_voltage = _curve(battery_ts, _BATTERY_CP, noise_sigma=0.03, seed=4, lo=0.0, hi=15.0)

    # engine_speed is derived from the same accelerator control points (plus
    # a small speed contribution) so it moves with accelerator_position, the
    # way a real engine-speed trace loosely tracks pedal input.
    accel_at_engine_rate = _curve(accel_ts, _ACCEL_CP, noise_sigma=1.5, seed=5, lo=0.0, hi=100.0)
    speed_at_engine_rate = np.interp(accel_ts, *zip(*_SPEED_CP))
    engine_speed = 800.0 + 38.0 * accel_at_engine_rate + 5.0 * speed_at_engine_rate
    rng = np.random.default_rng(6)
    engine_speed = np.clip(engine_speed + rng.normal(0.0, 15.0, size=engine_speed.shape), 700.0, 5200.0)

    return {
        "engine_speed": (Signal(samples=engine_speed, timestamps=accel_ts, name="engine_speed", unit="rpm"), "engine and pedal telemetry group"),
        "vehicle_speed": (Signal(samples=vehicle_speed, timestamps=speed_ts, name="vehicle_speed", unit="km/h"), "vehicle dynamics group"),
        "accelerator_position": (Signal(samples=accelerator_position, timestamps=accel_ts, name="accelerator_position", unit="%"), "engine and pedal telemetry group"),
        "brake_pressure": (Signal(samples=brake_pressure, timestamps=brake_ts, name="brake_pressure", unit="bar"), "brake system group"),
        "battery_voltage": (Signal(samples=battery_voltage, timestamps=battery_ts, name="battery_voltage", unit="V"), "electrical system group"),
    }


def main() -> None:
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    mdf = MDF()
    # engine_speed and accelerator_position share a sample rate and a comment
    # ("engine and pedal telemetry group") but are appended as separate
    # groups here, matching how independently-configured measurement
    # channels typically land in distinct channel groups on a real bus even
    # when they happen to share a sample rate.
    for name, (signal, comment) in build_channels().items():
        mdf.append([signal], comment=f"synthetic {comment} ({name})")

    mdf.save(OUTPUT_PATH, overwrite=True)
    print(f"Wrote synthetic fixture: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
