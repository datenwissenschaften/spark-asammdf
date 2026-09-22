package com.datenwissenschaften

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.sources._

class WhereClauseExtractorTest extends AnyFunSuite {
  test("extract empty filters") {
    assert(WhereClauseExtractor.extract(Array.empty) === "")
  }

  test("extract simple EqualTo filter") {
    val filters: Array[Filter] = Array(EqualTo("channel", "GPS_y"))
    assert(WhereClauseExtractor.extract(filters) === "channel = 'GPS_y'")
  }

  test("extract In filter") {
    val filters: Array[Filter] = Array(In("channel", Array("GPS_y", "GPS_x")))
    assert(WhereClauseExtractor.extract(filters) === "channel IN ('GPS_y', 'GPS_x')")
  }

  test("extract combined filters") {
    val filters: Array[Filter] = Array(
      EqualTo("channel", "GPS_y"),
      GreaterThan("time", 1.0)
    )
    assert(WhereClauseExtractor.extract(filters) === "channel = 'GPS_y' AND time > 1.0")
  }
}
