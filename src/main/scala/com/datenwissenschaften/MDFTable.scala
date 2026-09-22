package com.datenwissenschaften

import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util

/**
 * DSv2 [[Table]] wrapping a single MDF file path (`properties("path")`).
 *
 * Only [[TableCapability.BATCH_READ]] is advertised: this connector is read-only.
 */
class MDFTable(val mdfSchema: StructType, properties: util.Map[String, String])
    extends Table
    with org.apache.spark.sql.connector.catalog.SupportsRead {
  override def name(): String = "MDFTable"
  override def schema(): StructType = mdfSchema
  override def capabilities(): util.Set[TableCapability] = {
    val caps = new util.HashSet[TableCapability]()
    caps.add(TableCapability.BATCH_READ)
    caps
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new MDFScanBuilder(mdfSchema, options)
  }
}
