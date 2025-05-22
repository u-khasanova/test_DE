package org.example.fields

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DateTimeTest extends AnyFunSuite with Matchers {

  test("parse valid date in primary format") {
    val dt = DateTime.parse("08.11.2020_12:30:36")
    dt.get.date shouldBe "08.11.2020"
    dt.get.time shouldBe "12:30:36"
  }

  test("parse valid date in secondary format") {
    val dt = DateTime.parse("Mon,_08_Nov_2020_12:30:36_+0300")
    dt.get.date shouldBe "08.11.2020"
    dt.get.time shouldBe "12:30:36"
  }

  test("toString returns correct format") {
    val dt = DateTime("08.11.2020", "12:30:36")
    dt.toString shouldBe "08.11.2020 12:30:36"
  }
}
