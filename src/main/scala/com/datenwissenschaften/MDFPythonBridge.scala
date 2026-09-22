package com.datenwissenschaften

trait MDFPythonBridge {
  def getPartitions(options: java.util.Map[String, String], whereClause: String): java.util.List[String]
}
