"""One-off generator for notebooks/spark_asammdf_workflow.ipynb's cell structure.

This is a build-time authoring tool, not something the notebook or the connector depends on at
runtime. Run it, then execute the notebook with nbconvert (see `make example`) to populate
outputs and regenerate the docs/assets/spark-asammdf/*.png charts.
"""
import os

import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []


def md(text: str) -> None:
    cells.append(nbf.v4.new_markdown_cell(text.strip("\n")))


def code(text: str) -> None:
    cells.append(nbf.v4.new_code_cell(text.strip("\n")))


md(
    """
# spark-asammdf: synthetic MDF4 → Spark workflow

End-to-end demonstration of the `spark-asammdf` connector: a Spark DataSource V2
implementation that reads [ASAM MDF4](https://www.asam.net/standards/detail/mdf/) automotive
measurement files into Spark DataFrames.

**ASAM MDF (Measurement Data Format)** is the standard binary container used across the
automotive industry to log measurement-bus data (ECU signals, sensor channels, bus traces)
during vehicle testing and development. A single file typically holds many channels sampled at
different rates, organized into channel groups. Large-scale analysis of this data — across many
files and channels, with filtering and aggregation — is a natural fit for Apache Spark, but
MDF4 itself is a binary, non-splittable, non-Spark-native format.

`spark-asammdf` bridges the two: MDF4 parsing is delegated to the mature
[`asammdf`](https://github.com/danielhrisca/asammdf) Python library; the Spark-side integration
(DataSource V2, filter pushdown, partitioning) is implemented in Scala. Partition planning
happens once, on the driver, through a Py4J bridge (`mdf_spark.bridge`). Each partition is then
read *independently* on the executor by shelling out to `python -m mdf_spark.helper`, because in
a real (non-local) cluster the executor JVM is a separate process from the driver and cannot
call back into the driver's Py4J bridge — see `MDFPartitionReader.scala`.

> **SYNTHETIC DATA ONLY.** Everything read in this notebook comes from
> `src/test/resources/generate_fixture.py`: a small, seeded, deterministic synthetic drive
> cycle — stopped at a red light, a driver reaction delay after it turns green, then
> accelerate → cruise → brake → idle. No real vehicle, ECU, or customer data is involved
> anywhere in this repository.

**What's distributed vs. what stays local/file-oriented:**
- File-level metadata (channel names, units, groups) is read directly via `asammdf` below — a
  local, single-file operation, not a Spark job.
- Query *planning* (resolving which channel occurrences exist) happens once, driver-side.
- Reading channel *sample values* is distributed across Spark partitions — one partition per
  channel occurrence — with each executor invoking `asammdf` independently.
"""
)

md("## Setup")

code(
    """
import os
import sys

PROJECT_ROOT = os.path.abspath("..")
PYTHON_SRC = os.path.join(PROJECT_ROOT, "src/main/python")
JAR_PATH = os.path.join(PROJECT_ROOT, "target/scala-2.13/spark-asammdf-scala_2.13-0.1.jar")
FIXTURE_PATH = os.path.join(PROJECT_ROOT, "src/test/resources/data/sample.mf4")
ASSETS_DIR = os.path.join(PROJECT_ROOT, "docs/assets/spark-asammdf")

assert os.path.exists(JAR_PATH), f"Scala jar not found at {JAR_PATH}; run `sbt package` first"
assert os.path.exists(FIXTURE_PATH), (
    f"Synthetic fixture not found at {FIXTURE_PATH}; run "
    "`python src/test/resources/generate_fixture.py` first"
)

sys.path.insert(0, PYTHON_SRC)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYTHONPATH"] = PYTHON_SRC + os.pathsep + os.environ.get("PYTHONPATH", "")

os.makedirs(ASSETS_DIR, exist_ok=True)

# Print paths relative to the project root only, so this notebook's committed output never
# embeds the absolute filesystem layout of whichever machine last regenerated it.
print("Using jar:", os.path.relpath(JAR_PATH, PROJECT_ROOT))
print("Using fixture:", os.path.relpath(FIXTURE_PATH, PROJECT_ROOT))
"""
)

md(
    """
## Inspect the measurement file (local, file-oriented)

Before touching Spark, we inspect the MDF4 file's metadata directly through `asammdf` — this
step is local and file-oriented, not a distributed operation. It's the same library the
connector's Python helper (`mdf_spark.helper.get_partitions`) uses internally during query
planning.
"""
)

code(
    """
from asammdf import MDF
import pandas as pd

mdf = MDF(FIXTURE_PATH)

rows = []
for group_idx, group in enumerate(mdf.groups):
    for ch in group.channels:
        if ch.name == "time":
            continue  # master/time channel, not a data channel
        rows.append({
            "channel": ch.name,
            "group": group_idx,
            "unit": ch.unit,
            "samples": group.channel_group.cycles_nr,
        })

channel_overview = pd.DataFrame(rows).sort_values("channel").reset_index(drop=True)
channel_overview
"""
)

md(
    """
## Load into Spark

The same file, loaded through the actual `spark-asammdf` connector: a Spark DataFrame backed by
`com.datenwissenschaften.MDFDataSource`. `init_bridge` installs the driver-side Py4J bridge that
`MDFScan.planInputPartitions` (Scala) calls during query planning.
"""
)

code(
    """
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("spark-asammdf-notebook")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.jars", JAR_PATH)
    .config("spark.executorEnv.PYTHONPATH", os.environ["PYTHONPATH"])
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

from mdf_spark import init_bridge
init_bridge(spark)

df = (
    spark.read
    .format("com.datenwissenschaften.MDFDataSource")
    .option("path", FIXTURE_PATH)
    .load()
)
df.printSchema()
"""
)

md(
    """
## Explore: schema and channel/sample overview

The connector exposes a fixed, "long" schema — one row per `(time, channel)` sample, with
`valueNumeric` holding numeric physical values and `valueText` holding non-numeric ones — rather
than one column per channel. This keeps the schema stable across MDF files with different
channel sets, at the cost of needing an explicit pivot for wide, per-channel analysis (done
below where useful).

The cell below is the first "distributed computation → small aggregated result → Plotly"
pattern used throughout this notebook: `groupBy("channel").count()` runs as a Spark job; only
the five resulting rows are ever collected to the driver.
"""
)

code(
    """
channel_counts = (
    df.groupBy("channel").count()
    .orderBy("channel")
    .toPandas()
)
channel_counts
"""
)

code(
    """
import plotly.graph_objects as go

fig_overview = go.Figure(
    go.Bar(x=channel_counts["channel"], y=channel_counts["count"], marker_color="#3B82F6")
)
fig_overview.update_layout(
    title="Channel / sample overview (synthetic fixture)",
    xaxis_title="channel",
    yaxis_title="sample count",
    template="plotly_white",
    width=800, height=450,
)
fig_overview.write_image(os.path.join(ASSETS_DIR, "channel-overview.png"), scale=2)
fig_overview.show()
"""
)

md(
    """
## Analyze: hesitation at a traffic light

The fixture's first 15 seconds encode a small causal scenario, not just independent random
signals: the car is stopped at a red light, brake held. The light turns green — but the car
does not move instantly. There is a driver reaction delay before the brake is released and the
automatic transmission leaves neutral, then the accelerator is pressed, and only *after* all of
that does `vehicle_speed` actually leave 0. The gap between "light turns green" and "car
actually starts moving" is the interesting quantity here (a "hesitation gap"), and it only
exists because these five channels are read from the *same* file and can be reasoned about
together on a shared time axis.

Each `first_time(channel, condition)` call below is a real, independent Spark query (filter +
`orderBy("time")` + `first()`) against the connector — there is no client-side replay of the
generator's own control points; the timeline is reconstructed purely from what the connector
returns.
"""
)

code(
    """
from pyspark.sql import functions as F

def first_time(channel: str, condition: str):
    row = (
        df.where(f"channel = '{channel}'")
        .where(condition)
        .orderBy("time")
        .select("time")
        .first()
    )
    return row["time"]

t0 = df.where("channel = 'accelerator_position'").select(F.min("time")).first()[0]
t_green = first_time("traffic_light_state", "valueText = 'green'")
t_brake_released = first_time("brake_indicator", f"time >= '{t_green}' AND valueNumeric = 0")
t_gear_drive = first_time("gear_position", f"time >= '{t_green}' AND valueText = 'D'")
t_pedal_pressed = first_time("accelerator_position", f"time >= '{t_green}' AND valueNumeric > 5")
t_moving = first_time("vehicle_speed", f"time >= '{t_green}' AND valueNumeric > 0")

def since_green(t):
    return (t - t_green).total_seconds()

print(f"traffic light -> green:      t={t_green}")
print(f"brake released:              +{since_green(t_brake_released):.2f}s")
print(f"gear N -> D:                 +{since_green(t_gear_drive):.2f}s")
print(f"accelerator pressed (>5%):   +{since_green(t_pedal_pressed):.2f}s")
print(f"vehicle_speed > 0 (moving):  +{since_green(t_moving):.2f}s  <-- hesitation gap")
"""
)

code(
    """
# Pull the five channels for the first 15s onto a shared, relative time axis (t=0 at the
# window start) — a small, already-filtered slice of the file, not the whole fixture.
hesitation = (
    df.where(
        "channel IN ('vehicle_speed', 'accelerator_position', 'brake_indicator', 'gear_position')"
    )
    .withColumn("t_seconds", (F.unix_micros("time") - F.unix_micros(F.lit(t0))) / 1_000_000.0)
    .where("t_seconds < 15")
    .select("t_seconds", "channel", "valueNumeric", "valueText")
    .orderBy("t_seconds")
    .toPandas()
)

speed = hesitation[hesitation.channel == "vehicle_speed"]
accel = hesitation[hesitation.channel == "accelerator_position"]
brake = hesitation[hesitation.channel == "brake_indicator"]
gear = hesitation[hesitation.channel == "gear_position"]
"""
)

code(
    """
from plotly.subplots import make_subplots

def sec(t):
    return (t - t0).total_seconds()

fig_hesitation = make_subplots(
    rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.08,
    row_heights=[0.3, 0.25, 0.2, 0.2],
    subplot_titles=("vehicle_speed (km/h)", "accelerator_position (%)", "brake_indicator (0/1)", "gear_position"),
)
fig_hesitation.add_trace(go.Scatter(x=speed.t_seconds, y=speed.valueNumeric, mode="lines", line=dict(color="#3B82F6", width=2), name="vehicle_speed"), row=1, col=1)
fig_hesitation.add_trace(go.Scatter(x=accel.t_seconds, y=accel.valueNumeric, mode="lines", line=dict(color="#F59E0B", width=2), name="accelerator_position"), row=2, col=1)
fig_hesitation.add_trace(go.Scatter(x=brake.t_seconds, y=brake.valueNumeric, mode="lines", line=dict(shape="hv", color="#EF4444", width=2), name="brake_indicator"), row=3, col=1)
fig_hesitation.add_trace(go.Scatter(x=gear.t_seconds, y=gear.valueText, mode="lines", line=dict(shape="hv", color="#10B981", width=2), name="gear_position"), row=4, col=1)

events = [
    (sec(t_green), "green", "#16A34A"),
    (sec(t_brake_released), "brake released", "#6B7280"),
    (sec(t_moving), "car moving", "#2563EB"),
]
for x, _, color in events:
    fig_hesitation.add_vline(x=x, row="all", col=1, line_dash="dash", line_color=color, line_width=1.5)
for i, (x, label, color) in enumerate(events):
    fig_hesitation.add_annotation(
        x=x, y=1.0 - i * 0.045, xref="x1", yref="paper",
        text=label, showarrow=False, font=dict(color=color, size=11),
        xanchor="left", bgcolor="white",
    )

fig_hesitation.update_yaxes(row=4, col=1, categoryorder="array", categoryarray=["N", "D"])
fig_hesitation.update_layout(
    height=780, width=920, template="plotly_white", showlegend=False,
    title="Hesitation at a traffic light: reaction delay before the car starts moving (synthetic)",
    margin=dict(t=100),
)
fig_hesitation.write_image(os.path.join(ASSETS_DIR, "hesitation-timeline.png"), scale=2)
fig_hesitation.show()
"""
)

md(
    """
## Interpret

The hesitation gap (light green -> brake released -> gear engaged -> pedal pressed -> car moving)
is a direct artifact of how `generate_fixture.py` constructs the synthetic data, not a discovered
relationship: it's a deliberately encoded causal chain (see its `_TRAFFIC_LIGHT_STEPS` /
`_BRAKE_INDICATOR_STEPS` / `_GEAR_STEPS` / `_ACCEL_CP` / `_SPEED_CP` control points), reconstructed
above purely from Spark queries against the connector's output, not replayed from the generator.

No claim about real vehicle performance or real driver behavior should be drawn from it. The same
pipeline applied to a real MDF4 file would surface whatever timeline the underlying channels
actually contain.

**What this notebook does and does not demonstrate:**
- Demonstrates: real driver-side query planning, real executor-side subprocess reads, real
  filter pushdown, real discrete/string-channel reads (`traffic_light_state`, `gear_position`),
  and real distributed Spark aggregation, against a real (synthetic) MDF4 file.
- Does not demonstrate: native MDF parsing (delegated to `asammdf`), physical-unit propagation
  into the Spark schema (not currently implemented — see the README's Limitations section), or
  any multi-file / large-scale performance characteristic — this fixture is intentionally tiny.
"""
)

code("spark.stop()")

nb["cells"] = cells

out_path = os.path.join(os.path.dirname(__file__), "..", "notebooks", "spark_asammdf_workflow.ipynb")
with open(out_path, "w") as f:
    nbf.write(nb, f)
print(f"Wrote {os.path.abspath(out_path)}")
