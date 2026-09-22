package com.datenwissenschaften

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

/**
 * DSv2 [[Scan]]/[[Batch]]: plans one [[MDFPartition]] per resolved channel occurrence (or, if the read option
 * `maxRecordsPerPartition` is set, per record-range chunk of a large occurrence — see [[MDFPartition]]) by calling the
 * driver-side [[MDFPythonBridge]] once, then hands each partition to [[MDFPartitionReader]] for actual sample reading
 * on the executors. Partitioning is therefore across ''channels'' and their distinct occurrences by default; enabling
 * `maxRecordsPerPartition` additionally splits within a single large channel occurrence by time/record range.
 */
class MDFScan(schema: StructType, options: CaseInsensitiveStringMap, whereClause: String) extends Scan with Batch {
  private val logger = LoggerFactory.getLogger(classOf[MDFScan])

  override def readSchema(): StructType = schema
  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val bridge = MDFDataSource.getBridge
    if (bridge != null) {
      logger.info(s"Planning partitions for where clause: $whereClause")
      val parts = bridge.getPartitions(options, whereClause)
      logger.info(s"Resolved ${parts.size()} partitions")
      val result = new Array[InputPartition](parts.size())
      for (i <- 0 until parts.size()) {
        result(i) = MDFPartition(parts.get(i), options.get("path"), whereClause)
      }
      result
    } else {
      throw new RuntimeException("Python bridge not initialized. Please check your configuration.")
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new MDFPartitionReaderFactory(schema)
  }
}
