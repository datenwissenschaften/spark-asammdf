package com.datenwissenschaften

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Translates Spark's pushed-down filters into the SQL-like where-clause string that the Python helper understands (via
 * [[WhereClauseExtractor]]), and builds the resulting [[MDFScan]].
 *
 * Filters are advertised as pushed but not marked as fully handled, so Spark re-applies them after reading as a
 * correctness safety net — [[MDFPartitionReader]]'s best-effort filtering is therefore a pure I/O optimization (skip
 * channels/rows early), never a correctness requirement. Column pruning ([[pruneColumns]]) is honored: requesting only
 * `channel`, for example, lets [[MDFPartitionReader]] skip invoking the Python helper entirely for that partition.
 */
class MDFScanBuilder(schema: StructType, options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var pushed: Array[Filter] = Array.empty
  private var finalSchema: StructType = schema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushed = filters
    filters
  }

  override def pushedFilters(): Array[Filter] = pushed

  override def pruneColumns(requiredSchema: StructType): Unit = {
    finalSchema = requiredSchema
  }

  override def build(): Scan = {
    val whereClause = WhereClauseExtractor.extract(pushed)
    new MDFScan(finalSchema, options, whereClause)
  }
}
