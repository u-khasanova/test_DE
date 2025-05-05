package org.example.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateTimeParserSpec extends AnyFlatSpec with Matchers {

  private val validTestCases = Seq(
    ("01_Aug_2020_13:48:07", ("01.08.2020", "13:48:07")),
    ("Aug_01_2020_13:48:07", ("01.08.2020", "13:48:07")),
    ("Wed,_01_Aug_2020_13:48:07_+0300", ("01.08.2020", "13:48:07")),
    ("01.07.2020_13:42:01", ("01.07.2020", "13:42:01")),
    ("01-07-2020 13:42:01", ("01.07.2020", "13:42:01")),
//    ("07-01-2020 13:42:01", ("01.07.2020", "13:42:01")),
    ("2020-07-01 13:48:07", ("01.07.2020", "13:48:07")),
    ("2020-01-07 00:00:00", ("07.01.2020", "00:00:00")),
    ("07/01/2020 01:48:07 PM", ("01.07.2020", "13:48:07"))
  )

  private val invalidTestCases = Seq(
    "",
    " ",
    "InvalidDateString",
    "32_Jan_2020_25:61:61",
    "2020/07/01 13:48:07",
    "01_August_2020_13:48:07"
  )

  "DateTimeParser.parse()" should "correctly parse valid date formats" in {
    validTestCases.foreach { case (input, (expectedDate, expectedTime)) =>
      val result = DateTimeParser.parse(input)
      result shouldBe defined
      result.get.date shouldBe expectedDate
      result.get.time shouldBe expectedTime
    }
  }

  it should "return None for invalid date formats" in {
    invalidTestCases.foreach { input =>
      DateTimeParser.parse(input) shouldBe None
    }
  }

  it should "correctly handle day of week and timezone removal" in {
    val withDayAndTz = "Fri,_20_Mar_2020_11:45:25_+0300"
    val result = DateTimeParser.parse(withDayAndTz)
    result shouldBe defined
    result.get.date shouldBe "20.03.2020"
    result.get.time shouldBe "11:45:25"
  }

  it should "handle 24-hour time format correctly" in {
    val midnight = "01_Jan_2020_23:59:59"
    val result = DateTimeParser.parse(midnight)
    result shouldBe defined
    result.get.time shouldBe "23:59:59"
  }

  it should "handle AM/PM time format correctly" in {
    val testCases = Seq(
      ("07/01/2020 01:48:07 AM", "01:48:07"),
      ("07/01/2020 11:48:07 PM", "23:48:07"),
      ("07/01/2020 12:00:00 PM", "12:00:00"),
      ("07/01/2020 12:00:00 AM", "00:00:00")
    )

    testCases.foreach { case (input, expectedTime) =>
      val result = DateTimeParser.parse(input)
      result shouldBe defined
      result.get.time shouldBe expectedTime
    }
  }

  it should "handle different date separators" in {
    val testCases = Seq(
      ("01_Aug_2020_13:48:07", "_"),
      ("01-Aug-2020 13:48:07", "-"),
      ("01.Aug.2020_13:48:07", "."),
      ("01/Aug/2020 13:48:07", "/")
    )

    testCases.foreach { case (input, sep) =>
      val result = DateTimeParser.parse(input)
      withClue(s"For separator '$sep' in '$input'") {
        result shouldBe defined
      }
    }
  }

  it should "return None for null input" in {
    DateTimeParser.parse(null) shouldBe None
  }
}