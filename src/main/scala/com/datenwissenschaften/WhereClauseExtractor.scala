package com.datenwissenschaften

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources._

/**
 * Renders Spark's pushed-down [[Filter]] tree as the flat SQL-like where-clause string consumed by `mdf_spark.helper`
 * on the Python side. Only the filter kinds listed below are translated; anything else degrades to the always-true
 * `"1=1"` fallback, i.e. Spark still reads the data and then re-applies the original filter itself (see
 * [[MDFScanBuilder]]), so unsupported filters cost a missed I/O optimization, never a correctness bug.
 */
object WhereClauseExtractor {
  def extract(filters: Array[Filter]): String = {
    filters.map(filterToSql).mkString(" AND ")
  }

  private def filterToSql(filter: Filter): String = {
    filter match {
      case EqualTo(attribute, value)            => s"$attribute = ${formatValue(value)}"
      case GreaterThan(attribute, value)        => s"$attribute > ${formatValue(value)}"
      case GreaterThanOrEqual(attribute, value) => s"$attribute >= ${formatValue(value)}"
      case LessThan(attribute, value)           => s"$attribute < ${formatValue(value)}"
      case LessThanOrEqual(attribute, value)    => s"$attribute <= ${formatValue(value)}"
      case In(attribute, values)                => s"$attribute IN (${values.map(formatValue).mkString(", ")})"
      case IsNull(attribute)                    => s"$attribute IS NULL"
      case IsNotNull(attribute)                 => s"$attribute IS NOT NULL"
      case And(left, right)                     => s"(${filterToSql(left)} AND ${filterToSql(right)})"
      case Or(left, right)                      => s"(${filterToSql(left)} OR ${filterToSql(right)})"
      case Not(child)                           => s"NOT (${filterToSql(child)})"
      case StringStartsWith(attribute, value)   => s"$attribute LIKE '$value%'"
      case StringEndsWith(attribute, value)     => s"$attribute LIKE '%$value'"
      case StringContains(attribute, value)     => s"$attribute LIKE '%$value%'"
      case _                                    => "1=1" // Fallback
    }
  }

  private def formatValue(value: Any): String = {
    value match {
      case s: String => s"'$s'"
      case _         => value.toString
    }
  }
}
