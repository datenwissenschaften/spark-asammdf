package com.datenwissenschaften

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MDFScanBuilder(schema: StructType, options: CaseInsensitiveStringMap) 
  extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  
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
