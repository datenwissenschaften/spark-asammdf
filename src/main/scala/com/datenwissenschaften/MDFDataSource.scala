package com.datenwissenschaften

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util

/**
 * Holds the driver-side [[MDFPythonBridge]] singleton used for query planning.
 *
 * The bridge is installed once per driver JVM by `mdf_spark.init_bridge` (Python) before any read against
 * `com.datenwissenschaften.MDFDataSource` is planned. It is only ever read from the driver: partition ''reading'' (on
 * executors) never touches this object — see [[MDFPartitionReader]] for why.
 */
object MDFDataSource {
  private var bridge: MDFPythonBridge = null
  def setBridge(b: MDFPythonBridge): Unit = { bridge = b }
  def getBridge: MDFPythonBridge = bridge
}

/**
 * Spark DataSource V2 entry point for ASAM MDF4 files, registered as `"com.datenwissenschaften.MDFDataSource"`.
 *
 * The exposed schema is fixed and "long"/tall rather than one-column-per-channel: every row is a single `(time,
 * channel, valueNumeric, valueText)` sample. `valueNumeric` holds numeric physical values, `valueText` holds
 * non-numeric (string-converted) samples; exactly one of the two is non-null per row. This keeps the schema stable
 * across MDF files with different channel sets, at the cost of requiring a pivot for wide, per-channel analysis.
 */
class MDFDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    new StructType()
      .add("time", "timestamp")
      .add("channel", "string")
      .add("valueNumeric", "double")
      .add("valueText", "string")
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    new MDFTable(schema, properties)
  }
}
