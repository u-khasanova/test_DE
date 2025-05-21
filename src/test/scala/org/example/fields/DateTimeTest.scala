package org.example.fields

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DateTimeTest extends AnyFunSuite with Matchers {

  test("parse valid date in primary format") {
    val dt = DateTime.parse("08.11.2020_12:30:36")
    dt.date shouldBe "08.11.2020"
    dt.time shouldBe "12:30:36"
  }

  test("parse valid date in secondary format") {
    val dt = DateTime.parse("Mon,_08_Nov_2020_12:30:36_+0300")
    dt.date shouldBe "08.11.2020"
    dt.time shouldBe "12:30:36"
  }

  test("throw exception for null input") {
    intercept[DateParseException] {
      DateTime.parse(null)
    }.getMessage should include("Date string cannot be null or empty")
  }

  test("throw exception for empty input") {
    intercept[DateParseException] {
      DateTime.parse("")
    }.getMessage should include("Date string cannot be null or empty")
  }

  test("throw exception for invalid format") {
    intercept[DateParseException] {
      DateTime.parse("2020/11/08 12:30:36")
    }.getMessage should include("Unrecognized date format")
  }

  test("toString returns correct format") {
    val dt = DateTime("08.11.2020", "12:30:36")
    dt.toString shouldBe "08.11.2020 12:30:36"
  }
}
