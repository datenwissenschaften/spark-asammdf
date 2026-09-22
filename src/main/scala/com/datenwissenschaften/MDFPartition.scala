package com.datenwissenschaften

import org.apache.spark.sql.connector.read.InputPartition

case class MDFPartition(data: String, filePath: String, whereClause: String) extends InputPartition
