package com.datenwissenschaften

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

class MDFPartitionReaderFactory(readSchema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new MDFPartitionReader(partition.asInstanceOf[MDFPartition], readSchema)
  }
}
