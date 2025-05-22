package org.example.events

import org.example.fields.DateTime
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class SessionTest extends AnyFunSuite {

  test("parseStart should extract date from line") {
    val startDate = DateTime.parse("01.09.2020_06:44:35")
    val lines = mutable
      .ArrayBuffer("SESSION_START 01.09.2020_06:44:35")
      .iterator
      .buffered

    val result = Session.parseStart(lines)
    assert(result.isInstanceOf[Option[DateTime]])
    assert(result == startDate)
  }

  test("parseStart should return None for invalid format") {
    val lines = mutable
      .ArrayBuffer("SESSION_START INVALID_DATE_FORMAT")
      .iterator
      .buffered

    val result = Session.parseStart(lines)
    assert(result.isEmpty)
  }

  test("parseEnd should extract date from line") {
    val endDate = DateTime.parse("01.09.2020_06:51:02")
    val lines = mutable
      .ArrayBuffer("SESSION_END 01.09.2020_06:51:02")
      .iterator
      .buffered

    val result = Session.parseStart(lines)
    assert(result.isInstanceOf[Option[DateTime]])
    assert(result == endDate)
  }
}
