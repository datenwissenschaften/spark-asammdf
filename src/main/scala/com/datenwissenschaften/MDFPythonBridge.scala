package com.datenwissenschaften

/**
 * Java/Py4J-facing contract implemented in Python (`mdf_spark.bridge.PythonBridge`) and called from the driver only,
 * during query planning (see [[MDFScan.planInputPartitions]]).
 *
 * Each returned partition descriptor is a `"channel|group|index|offset|count"` string identifying one `[offset, offset
 * + count)` record range of one channel occurrence, already resolved against the MDF file's channel/group metadata
 * (and, via the optional `maxRecordsPerPartition` read option, pre-split into multiple ranges for large occurrences —
 * see `mdf_spark.helper.get_partitions`). Reading the actual sample values for that range happens later, on the
 * executor, and does not go through this bridge — see [[MDFPartitionReader]].
 */
trait MDFPythonBridge {
  def getPartitions(options: java.util.Map[String, String], whereClause: String): java.util.List[String]
}
