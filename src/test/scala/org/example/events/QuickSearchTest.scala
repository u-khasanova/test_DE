package org.example.events

import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import scala.collection.mutable

class QuickSearchTest extends AnyFunSuite {

  test("parse valid QS") {
    val lines = mutable
      .ArrayBuffer(
        "QS 08.11.2020_12:30:36 {организация осуществления уплаты}",
        "187920943 DOC1 DOC2"
      )
      .iterator
      .buffered
    val qs = QuickSearch.parse(lines)

    assert(qs.isInstanceOf[QuickSearch])
    assert(qs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${qs.date.get.toLocalDate}" == "2020-11-08")
    assert(s"${qs.date.get.toLocalTime}" == "12:30:36")
    assert(qs.id.isInstanceOf[Option[Int]])
    assert(qs.id.contains(187920943))
    assert(qs.query.isInstanceOf[String])
    assert(qs.query == "организация осуществления уплаты")
    assert(qs.docIds.isInstanceOf[List[String]])
    assert(qs.docIds == List("DOC1", "DOC2"))
  }

  test("handle empty date") {
    val lines = mutable
      .ArrayBuffer(
        "QS {организация осуществления уплаты}",
        "187920943 DOC1 DOC2"
      )
      .iterator
      .buffered
    val qs = QuickSearch.parse(lines)

    assert(qs.isInstanceOf[QuickSearch])
    assert(qs.date.isEmpty)
    assert(qs.id.isInstanceOf[Option[Int]])
    assert(qs.id.contains(187920943))
    assert(qs.query.isInstanceOf[String])
    assert(qs.query == "организация осуществления уплаты")
    assert(qs.docIds.isInstanceOf[List[String]])
    assert(qs.docIds == List("DOC1", "DOC2"))
  }

  test("handle invalid date") {
    val lines = mutable
      .ArrayBuffer(
        "QS INVALID_DATE {организация осуществления уплаты}",
        "187920943 DOC1 DOC2"
      )
      .iterator
      .buffered
    val qs = QuickSearch.parse(lines)

    assert(qs.isInstanceOf[QuickSearch])
    assert(qs.date.isEmpty)
    assert(qs.id.isInstanceOf[Option[Int]])
    assert(qs.id.contains(187920943))
    assert(qs.query.isInstanceOf[String])
    assert(qs.query == "организация осуществления уплаты")
    assert(qs.docIds.isInstanceOf[List[String]])
    assert(qs.docIds == List("DOC1", "DOC2"))
  }

  test("handle date secondary format") {
    val lines = mutable
      .ArrayBuffer(
        "QS Wed,_08_Nov_2020_12:30:36_+0300 {организация осуществления уплаты}",
        "187920943 DOC1 DOC2"
      )
      .iterator
      .buffered
    val qs = QuickSearch.parse(lines)

    assert(qs.isInstanceOf[QuickSearch])
    assert(qs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${qs.date.get.toLocalDate}" == "2020-11-08")
    assert(s"${qs.date.get.toLocalTime}" == "12:30:36")
    assert(qs.id.isInstanceOf[Option[Int]])
    assert(qs.id.contains(187920943))
    assert(qs.query.isInstanceOf[String])
    assert(qs.query == "организация осуществления уплаты")
    assert(qs.docIds.isInstanceOf[List[String]])
    assert(qs.docIds == List("DOC1", "DOC2"))
  }

  test("handle empty query") {
    val lines = mutable
      .ArrayBuffer(
        "QS 08.11.2020_12:30:36 {}",
        "187920943 DOC1 DOC2"
      )
      .iterator
      .buffered
    val qs = QuickSearch.parse(lines)

    assert(qs.isInstanceOf[QuickSearch])
    assert(qs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${qs.date.get.toLocalDate}" == "2020-11-08")
    assert(s"${qs.date.get.toLocalTime}" == "12:30:36")
    assert(qs.id.isInstanceOf[Option[Int]])
    assert(qs.id.contains(187920943))
    assert(qs.query.isEmpty)
    assert(qs.docIds.isInstanceOf[List[String]])
    assert(qs.docIds == List("DOC1", "DOC2"))
  }

  test("handle empty ID") {
    val lines = mutable
      .ArrayBuffer(
        "QS 08.11.2020_12:30:36 {организация осуществления уплаты}",
        "DOC1 DOC2"
      )
      .iterator
      .buffered
    val qs = QuickSearch.parse(lines)

    assert(qs.isInstanceOf[QuickSearch])
    assert(qs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${qs.date.get.toLocalDate}" == "2020-11-08")
    assert(s"${qs.date.get.toLocalTime}" == "12:30:36")
    assert(qs.id.isEmpty)
    assert(qs.query.isInstanceOf[String])
    assert(qs.query == "организация осуществления уплаты")
    assert(qs.docIds.isInstanceOf[List[String]])
    assert(qs.docIds == List("DOC1", "DOC2"))
  }

  test("handle negative ID") {
    val lines = mutable
      .ArrayBuffer(
        "QS 08.11.2020_12:30:36 {организация осуществления уплаты}",
        "-187920943 DOC1 DOC2"
      )
      .iterator
      .buffered
    val qs = QuickSearch.parse(lines)

    assert(qs.isInstanceOf[QuickSearch])
    assert(qs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${qs.date.get.toLocalDate}" == "2020-11-08")
    assert(s"${qs.date.get.toLocalTime}" == "12:30:36")
    assert(qs.id.isInstanceOf[Option[Int]])
    assert(qs.id.contains(187920943))
    assert(qs.query.isInstanceOf[String])
    assert(qs.query == "организация осуществления уплаты")
    assert(qs.docIds.isInstanceOf[List[String]])
    assert(qs.docIds == List("DOC1", "DOC2"))
  }

  test("handle empty search result") {
    val lines = mutable
      .ArrayBuffer(
        "QS 08.11.2020_12:30:36 {организация осуществления уплаты}",
        "187920943"
      )
      .iterator
      .buffered
    val qs = QuickSearch.parse(lines)

    assert(qs.isInstanceOf[QuickSearch])
    assert(qs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${qs.date.get.toLocalDate}" == "2020-11-08")
    assert(s"${qs.date.get.toLocalTime}" == "12:30:36")
    assert(qs.id.isInstanceOf[Option[Int]])
    assert(qs.id.contains(187920943))
    assert(qs.query.isInstanceOf[String])
    assert(qs.query == "организация осуществления уплаты")
    assert(qs.docIds.isEmpty)
  }
}
