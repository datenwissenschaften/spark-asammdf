"""Generates the synthetic MDF4 fixture used by the test suite.

Run this script whenever the fixture needs to be regenerated:

    python src/test/resources/generate_fixture.py

The fixture contains only synthetic, generic channel names (no real vehicle
or project data) so it is safe to commit to the repository and to a public
GitHub history.
"""
import os

import numpy as np
from asammdf import MDF, Signal

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "data", "sample.mf4")

# Synthetic ADAS-style boolean flags, each logged on its own "bus" (group).
CHANNELS = {
    "ObstacleDetected": 0.05,
    "LaneChangePossible": 0.1,
    "BrakePressureWarning": 0.2,
    "CruiseControlActive": 0.5,
}


def build_signal(name: str, duration_s: float, step_s: float, seed: int) -> Signal:
    rng = np.random.default_rng(seed)
    timestamps = np.arange(0.0, duration_s, step_s)
    samples = (rng.random(len(timestamps)) < 0.5).astype(np.uint8)
    return Signal(samples=samples, timestamps=timestamps, name=name, unit="")


def main() -> None:
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    mdf = MDF()
    for i, (name, step_s) in enumerate(CHANNELS.items()):
        sig = build_signal(name, duration_s=60.0, step_s=step_s, seed=i)
        mdf.append([sig], comment=f"synthetic group for {name}")

    mdf.save(OUTPUT_PATH, overwrite=True)
    print(f"Wrote synthetic fixture: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
