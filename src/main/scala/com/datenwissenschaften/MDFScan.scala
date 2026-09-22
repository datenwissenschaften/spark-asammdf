package com.datenwissenschaften

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MDFScan(schema: StructType, options: CaseInsensitiveStringMap, whereClause: String) extends Scan with Batch {
  override def readSchema(): StructType = schema
  override def toBatch: Batch = this
  
  override def planInputPartitions(): Array[InputPartition] = {
    val bridge = MDFDataSource.getBridge
    if (bridge != null) {
      println(s"Get partitions - $whereClause")
      val parts = bridge.getPartitions(options, whereClause)
      println(s"Got ${parts.size()} partitions")
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
