package com.datenwissenschaften

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources._

object WhereClauseExtractor {
  def extract(filters: Array[Filter]): String = {
    filters.map(filterToSql).mkString(" AND ")
  }

  private def filterToSql(filter: Filter): String = {
    filter match {
      case EqualTo(attribute, value) => s"$attribute = ${formatValue(value)}"
      case GreaterThan(attribute, value) => s"$attribute > ${formatValue(value)}"
      case GreaterThanOrEqual(attribute, value) => s"$attribute >= ${formatValue(value)}"
      case LessThan(attribute, value) => s"$attribute < ${formatValue(value)}"
      case LessThanOrEqual(attribute, value) => s"$attribute <= ${formatValue(value)}"
      case In(attribute, values) => s"$attribute IN (${values.map(formatValue).mkString(", ")})"
      case IsNull(attribute) => s"$attribute IS NULL"
      case IsNotNull(attribute) => s"$attribute IS NOT NULL"
      case And(left, right) => s"(${filterToSql(left)} AND ${filterToSql(right)})"
      case Or(left, right) => s"(${filterToSql(left)} OR ${filterToSql(right)})"
      case Not(child) => s"NOT (${filterToSql(child)})"
      case StringStartsWith(attribute, value) => s"$attribute LIKE '$value%'"
      case StringEndsWith(attribute, value) => s"$attribute LIKE '%$value'"
      case StringContains(attribute, value) => s"$attribute LIKE '%$value%'"
      case _ => "1=1" // Fallback
    }
  }

  private def formatValue(value: Any): String = {
    value match {
      case s: String => s"'$s'"
      case _ => value.toString
    }
  }
}
