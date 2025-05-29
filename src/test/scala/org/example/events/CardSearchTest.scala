package org.example.events

import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import scala.collection.mutable

class CardSearchTest extends AnyFunSuite {

  test("parse valid card search") {
    val lines = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 16.05.2020_00:26:28",
        "$134 оквэды",
        "CARD_SEARCH_END",
        "1125421 RZR_144138 RZR_183391"
      )
      .iterator
      .buffered

    val cs = CardSearch.parse(lines)

    assert(cs.isInstanceOf[CardSearch])
    assert(cs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${cs.date.get.toLocalDate}" == "2020-05-16")
    assert(s"${cs.date.get.toLocalTime}" == "00:26:28")
    assert(cs.id.isInstanceOf[Option[Int]])
    assert(cs.id.contains(1125421))
    assert(cs.query.isInstanceOf[String])
    assert(cs.query == "$134 оквэды")
    assert(cs.docIds.isInstanceOf[List[String]])
    assert(cs.docIds == List("RZR_144138", "RZR_183391"))
  }

  test("handle empty date") {
    val lines = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START",
        "$134 оквэды",
        "$134 приказ 796 от27.12.2011",
        "CARD_SEARCH_END",
        "1125421 RZR_144138 RZR_183391"
      )
      .iterator
      .buffered

    val cs = CardSearch.parse(lines)

    assert(cs.isInstanceOf[CardSearch])
    assert(cs.date.isEmpty)
    assert(cs.id.isInstanceOf[Option[Int]])
    assert(cs.id.contains(1125421))
    assert(cs.query.isInstanceOf[String])
    assert(cs.query == "$134 оквэды $134 приказ 796 от27.12.2011")
    assert(cs.docIds.isInstanceOf[List[String]])
    assert(cs.docIds == List("RZR_144138", "RZR_183391"))
  }

  test("handle invalid date") {
    val lines = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START",
        "$134 оквэды",
        "CARD_SEARCH_END",
        "1125421 RZR_144138 RZR_183391"
      )
      .iterator
      .buffered

    val cs = CardSearch.parse(lines)

    assert(cs.isInstanceOf[CardSearch])
    assert(cs.date.isEmpty)
    assert(cs.id.isInstanceOf[Option[Int]])
    assert(cs.id.contains(1125421))
    assert(cs.query.isInstanceOf[String])
    assert(cs.query == "$134 оквэды")
    assert(cs.docIds.isInstanceOf[List[String]])
    assert(cs.docIds == List("RZR_144138", "RZR_183391"))
  }

  test("handle date secondary format") {
    val lines = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START Sat,_16_May_2020_00:26:28_+0300",
        "$134 оквэды",
        "CARD_SEARCH_END",
        "1125421 RZR_144138 RZR_183391"
      )
      .iterator
      .buffered

    val cs = CardSearch.parse(lines)

    assert(cs.isInstanceOf[CardSearch])
    assert(cs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${cs.date.get.toLocalDate}" == "2020-05-16")
    assert(s"${cs.date.get.toLocalTime}" == "00:26:28")
    assert(cs.id.isInstanceOf[Option[Int]])
    assert(cs.id.contains(1125421))
    assert(cs.query.isInstanceOf[String])
    assert(cs.query == "$134 оквэды")
    assert(cs.docIds.isInstanceOf[List[String]])
    assert(cs.docIds == List("RZR_144138", "RZR_183391"))
  }

  test("handle empty query") {
    val lines = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 16.05.2020_00:26:28",
        "CARD_SEARCH_END",
        "1125421 RZR_144138 RZR_183391"
      )
      .iterator
      .buffered

    val cs = CardSearch.parse(lines)
    assert(cs.isInstanceOf[CardSearch])
    assert(cs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${cs.date.get.toLocalDate}" == "2020-05-16")
    assert(s"${cs.date.get.toLocalTime}" == "00:26:28")
    assert(cs.id.isInstanceOf[Option[Int]])
    assert(cs.id.contains(1125421))
    assert(cs.query.isEmpty)
    assert(cs.docIds.isInstanceOf[List[String]])
    assert(cs.docIds == List("RZR_144138", "RZR_183391"))
  }

  test("handle empty ID") {
    val lines = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 16.05.2020_00:26:28",
        "$134 оквэды",
        "CARD_SEARCH_END",
        "RZR_144138 RZR_183391"
      )
      .iterator
      .buffered

    val cs = CardSearch.parse(lines)

    assert(cs.isInstanceOf[CardSearch])
    assert(cs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${cs.date.get.toLocalDate}" == "2020-05-16")
    assert(s"${cs.date.get.toLocalTime}" == "00:26:28")
    assert(cs.id.isEmpty)
    assert(cs.query.isInstanceOf[String])
    assert(cs.query == "$134 оквэды")
    assert(cs.docIds.isInstanceOf[List[String]])
    assert(cs.docIds == List("RZR_144138", "RZR_183391"))
  }

  test("handle negative ID") {
    val lines = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 16.05.2020_00:26:28",
        "$134 оквэды",
        "CARD_SEARCH_END",
        "-1125421 RZR_144138 RZR_183391"
      )
      .iterator
      .buffered

    val cs = CardSearch.parse(lines)

    assert(cs.isInstanceOf[CardSearch])
    assert(cs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${cs.date.get.toLocalDate}" == "2020-05-16")
    assert(s"${cs.date.get.toLocalTime}" == "00:26:28")
    assert(cs.id.isInstanceOf[Option[Int]])
    assert(cs.id.contains(1125421))
    assert(cs.query.isInstanceOf[String])
    assert(cs.query == "$134 оквэды")
    assert(cs.docIds.isInstanceOf[List[String]])
    assert(cs.docIds == List("RZR_144138", "RZR_183391"))
  }

  test("handle empty search result") {
    val lines = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 16.05.2020_00:26:28",
        "$134 оквэды",
        "CARD_SEARCH_END",
        "1125421"
      )
      .iterator
      .buffered

    val cs = CardSearch.parse(lines)

    assert(cs.isInstanceOf[CardSearch])
    assert(cs.date.isInstanceOf[Option[LocalDateTime]])
    assert(s"${cs.date.get.toLocalDate}" == "2020-05-16")
    assert(s"${cs.date.get.toLocalTime}" == "00:26:28")
    assert(cs.id.isInstanceOf[Option[Int]])
    assert(cs.id.contains(1125421))
    assert(cs.query.isInstanceOf[String])
    assert(cs.query == "$134 оквэды")
    assert(cs.docIds.isEmpty)
  }
}
