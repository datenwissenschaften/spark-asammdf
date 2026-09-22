package com.datenwissenschaften

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util

object MDFDataSource {
  private var bridge: MDFPythonBridge = null
  def setBridge(b: MDFPythonBridge): Unit = { bridge = b }
  def getBridge: MDFPythonBridge = bridge
}

class MDFDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    new StructType()
      .add("time", "timestamp")
      .add("channel", "string")
      .add("valueCont", "double")
      .add("valueDisc", "string")
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new MDFTable(schema, properties)
  }
}
