from mdf_spark.helper import get_partitions

class PythonBridge(object):
    """Implements MDFPythonBridge for the driver-side query-planning call
    only (getPartitions). Reading channel values happens per-partition on
    the executors, which invoke mdf_spark.helper as their own subprocess —
    see MDFPartitionReader.scala — since a real cluster's executors run in
    separate JVMs from the driver and can't call back into this bridge."""

    def __init__(self, spark):
        self.spark = spark

    def getPartitions(self, options, where_clause):
        path = options.get("path")
        # Optional "maxRecordsPerPartition" read option: splits any channel
        # occurrence with more records than this into multiple time-range
        # partitions instead of one partition per whole occurrence — see
        # mdf_spark.helper.get_partitions / _record_chunks.
        max_records_raw = options.get("maxRecordsPerPartition")
        max_records = int(max_records_raw) if max_records_raw else None
        parts = get_partitions(path, where_clause, max_records)

        # Convert to Java List of Strings for Scala
        gateway = self.spark._sc._gateway
        java_list = gateway.jvm.java.util.ArrayList()
        for p in parts:
            java_list.add(f"{p['channel']}|{p['group']}|{p['index']}|{p['offset']}|{p['count']}")
        return java_list

    class Java:
        implements = ["com.datenwissenschaften.MDFPythonBridge"]


def init_bridge(spark):
    bridge = PythonBridge(spark)

    # Py4J stores Python callback objects through weak references. Keep a
    # strong reference alive for at least as long as the SparkSession;
    # otherwise the bridge may be garbage-collected after this function
    # returns and a later lazy Spark action fails with "Connection refused"
    # while planning MDF partitions.
    spark._mdf_python_bridge = bridge

    spark._jvm.com.datenwissenschaften.MDFDataSource.setBridge(bridge)
