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
        parts = get_partitions(path, where_clause)

        # Convert to Java List of Strings for Scala
        gateway = self.spark._sc._gateway
        java_list = gateway.jvm.java.util.ArrayList()
        for p in parts:
            java_list.add(f"{p['channel']}|{p['group']}|{p['index']}|{p['count']}")
        return java_list

    class Java:
        implements = ["com.datenwissenschaften.MDFPythonBridge"]


def init_bridge(spark):
    bridge = PythonBridge(spark)
    spark._jvm.com.datenwissenschaften.MDFDataSource.setBridge(bridge)
