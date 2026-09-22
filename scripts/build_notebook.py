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
> `src/test/resources/generate_fixture.py`: a small, seeded, deterministic synthetic "drive
> cycle" (accelerate → cruise → brake → idle). No real vehicle, ECU, or customer data is
> involved anywhere in this repository.

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
## Analyze: synthetic signals over time

`engine_speed` and `vehicle_speed` are selected via a pushed-down `channel IN (...)` predicate
— `WhereClauseExtractor` (Scala) renders it into the where-clause string the Python helper
receives — then collected directly. That's safe here because the synthetic fixture is small
(3,600 rows across both channels combined); a larger file would typically be aggregated or
downsampled in Spark before collecting.
"""
)

code(
    """
signals = (
    df.where("channel IN ('engine_speed', 'vehicle_speed')")
    .select("time", "channel", "valueNumeric")
    .orderBy("time")
    .toPandas()
)

engine = signals[signals["channel"] == "engine_speed"]
speed = signals[signals["channel"] == "vehicle_speed"]

from plotly.subplots import make_subplots

fig_signals = make_subplots(specs=[[{"secondary_y": True}]])
fig_signals.add_trace(
    go.Scatter(x=engine["time"], y=engine["valueNumeric"], name="engine_speed (rpm)",
               line=dict(color="#EF4444")),
    secondary_y=False,
)
fig_signals.add_trace(
    go.Scatter(x=speed["time"], y=speed["valueNumeric"], name="vehicle_speed (km/h)",
               line=dict(color="#3B82F6")),
    secondary_y=True,
)
fig_signals.update_layout(
    title="Synthetic signals over time: engine_speed vs. vehicle_speed",
    template="plotly_white",
    width=900, height=500,
)
fig_signals.update_yaxes(title_text="engine_speed (rpm)", secondary_y=False)
fig_signals.update_yaxes(title_text="vehicle_speed (km/h)", secondary_y=True)
fig_signals.write_image(os.path.join(ASSETS_DIR, "synthetic-signals.png"), scale=2)
fig_signals.show()
"""
)

md(
    """
## Analyze: accelerator position vs. vehicle speed

`accelerator_position` (50 Hz) and `vehicle_speed` (10 Hz) are sampled at different rates, so
comparing them requires aligning them onto a shared time grid — a genuine distributed
aggregation, not a local join. We bucket both channels into 0.5s windows with a Spark
`groupBy(...).pivot(...).avg(...)`, which runs as a single Spark job and returns one small row
per time bucket (~120 rows for this 60s fixture).
"""
)

code(
    """
from pyspark.sql import functions as F

bucket_micros = (F.floor(F.unix_micros("time") / 500_000) * 500_000).cast("long")

bucketed = (
    df.where("channel IN ('accelerator_position', 'vehicle_speed')")
    .withColumn("bucket", F.timestamp_micros(bucket_micros))
    .groupBy("bucket")
    .pivot("channel", ["accelerator_position", "vehicle_speed"])
    .agg(F.avg("valueNumeric"))
    .orderBy("bucket")
    .na.drop()
    .toPandas()
)
bucketed.head()
"""
)

code(
    """
fig_accel_speed = go.Figure(
    go.Scatter(
        x=bucketed["accelerator_position"],
        y=bucketed["vehicle_speed"],
        mode="lines+markers",
        marker=dict(
            size=6, color=bucketed.index, colorscale="Viridis",
            showscale=True, colorbar=dict(title="time bucket #"),
        ),
        line=dict(color="rgba(100,100,100,0.3)"),
    )
)
fig_accel_speed.update_layout(
    title="accelerator_position vs. vehicle_speed (0.5s buckets, synthetic drive cycle)",
    xaxis_title="accelerator_position (%, bucket avg)",
    yaxis_title="vehicle_speed (km/h, bucket avg)",
    template="plotly_white",
    width=800, height=500,
)
fig_accel_speed.write_image(os.path.join(ASSETS_DIR, "accelerator-vs-speed.png"), scale=2)
fig_accel_speed.show()
"""
)

md(
    """
## Interpret

The pattern above — high accelerator input while speed is still ramping up, lower (but
non-zero) accelerator input during the cruise phase, and near-zero accelerator input while
speed falls during the braking phase — is a direct artifact of how `generate_fixture.py`
constructs the synthetic drive cycle (see its `_ACCEL_CP` / `_SPEED_CP` control points). It is
not a discovered relationship, and no claim about real vehicle performance should be drawn from
it. The same pipeline applied to a real MDF4 file would surface whatever relationship the
underlying channels actually contain.

**What this notebook does and does not demonstrate:**
- Demonstrates: real driver-side query planning, real executor-side subprocess reads, real
  filter pushdown, and real distributed Spark aggregation, against a real (synthetic) MDF4 file.
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
