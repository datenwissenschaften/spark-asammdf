package com.datenwissenschaften

import org.apache.spark.sql.connector.read.InputPartition

/**
 * One planned unit of work: a single `"channel|group|index|offset|count"` record-range descriptor resolved on the
 * driver (see [[MDFPythonBridge]]), plus the source file path and where-clause needed to read it independently on the
 * executor. `offset`/`count` identify a `[offset, offset + count)` slice of that channel occurrence's records —
 * normally the whole occurrence (`offset = 0`), but when the `maxRecordsPerPartition` read option is set, a single
 * large occurrence is split into several of these instead, one per record range, each read independently. Serialized to
 * executors by Spark as-is.
 */
case class MDFPartition(data: String, filePath: String, whereClause: String) extends InputPartition
