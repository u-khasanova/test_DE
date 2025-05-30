package org.example.processors

import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.processors.fixers.DocOpenMapper
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DocOpenMapperTest extends AnyFunSuite {

  test("should correctly map DocOpens to QuickSearches by ID") {
    val testDate = Some(
      LocalDateTime.parse(
        "08.11.2020_12:29:47",
        DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
      )
    )
    val doc1 = DocOpen(testDate, Some(1), Some("doc1"))
    val doc2 = DocOpen(testDate, Some(2), Some("doc2"))
    val doc3 = DocOpen(testDate, Some(99), Some("doc3"))
    val qs1 =
      QuickSearch(testDate, Some(1), "query", List("doc1"))
    val qs2 =
      QuickSearch(testDate, Some(2), "query", List("doc2"))

    val session = Session(
      "test.txt",
      testDate,
      testDate,
      quickSearches = List(qs1, qs2),
      cardSearches = List.empty,
      docOpens = List(doc1, doc2, doc3)
    )

    val result = DocOpenMapper.mapDocOpens(session)

    assert(
      result.quickSearches.exists(
        _.id.contains(1) &&
          result.quickSearches
            .find(_.id.contains(1))
            .get
            .docOpens
            .contains(doc1)
      )
    )
    assert(
      result.quickSearches.exists(_.id.contains(2)) &&
        result.quickSearches.find(_.id.contains(2)).get.docOpens.contains(doc2)
    )
    assert(result.docOpens.contains(doc3))
  }

  test("should correctly map DocOpens to CardSearches by ID") {
    val testDate = Some(
      LocalDateTime.parse(
        "08.11.2020_12:29:47",
        DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
      )
    )
    val doc1 = DocOpen(testDate, Some(1), Some("doc1"))
    val doc3 = DocOpen(testDate, Some(99), Some("doc3"))
    val cs1 =
      CardSearch(testDate, Some(1), "query", List("doc1"))

    val session = Session(
      "test.txt",
      testDate,
      testDate,
      quickSearches = List.empty,
      cardSearches = List(cs1),
      docOpens = List(doc1, doc3)
    )

    val result = DocOpenMapper.mapDocOpens(session)

    assert(result.cardSearches.head.docOpens.contains(doc1))
    assert(result.docOpens.contains(doc3))
  }

  test("should not map DocOpens without ID") {
    val testDate = Some(
      LocalDateTime.parse(
        "08.11.2020_12:29:47",
        DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
      )
    )
    val doc4 = DocOpen(testDate, None, Some("doc4"))
    val qs1 =
      QuickSearch(testDate, Some(1), "query", List("doc1"))

    val session = Session(
      "test.txt",
      testDate,
      testDate,
      quickSearches = List(qs1),
      cardSearches = List.empty,
      docOpens = List(doc4)
    )

    val result = DocOpenMapper.mapDocOpens(session)
    assert(result.docOpens.contains(doc4))
    assert(result.quickSearches.head.docOpens.isEmpty)
  }

  test("should not map to searches without ID") {
    val testDate = Some(
      LocalDateTime.parse(
        "08.11.2020_12:29:47",
        DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
      )
    )
    val doc1 = DocOpen(testDate, Some(1), Some("doc1"))
    val qs3 =
      QuickSearch(testDate, None, "query", List("doc4"))

    val session = Session(
      "test.txt",
      testDate,
      testDate,
      quickSearches = List(qs3),
      cardSearches = List.empty,
      docOpens = List(doc1)
    )

    val result = DocOpenMapper.mapDocOpens(session)
    assert(result.docOpens.contains(doc1))
    assert(result.quickSearches.head.docOpens.isEmpty)
  }

  test("should handle empty sessions correctly") {
    val testDate = Some(
      LocalDateTime.parse(
        "08.11.2020_12:29:47",
        DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
      )
    )

    val emptySession = Session(
      "empty.txt",
      testDate,
      testDate,
      quickSearches = List.empty,
      cardSearches = List.empty,
      docOpens = List.empty
    )

    val result = DocOpenMapper.mapDocOpens(emptySession)
    assert(result.quickSearches.isEmpty)
    assert(result.cardSearches.isEmpty)
    assert(result.docOpens.isEmpty)
  }

  test("should preserve original searches when no matches found") {
    val testDate = Some(
      LocalDateTime.parse(
        "08.11.2020_12:29:47",
        DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
      )
    )
    val doc3 = DocOpen(testDate, Some(99), Some("doc3"))
    val qs1 =
      QuickSearch(testDate, Some(1), "query", List("doc1"))
    val cs1 =
      CardSearch(testDate, Some(1), "query", List("doc1"))

    val session = Session(
      "test.txt",
      testDate,
      testDate,
      quickSearches = List(qs1),
      cardSearches = List(cs1),
      docOpens = List(doc3)
    )

    val result = DocOpenMapper.mapDocOpens(session)
    assert(result.quickSearches.size == 1)
    assert(result.cardSearches.size == 1)
    assert(result.docOpens.contains(doc3))
    assert(result.quickSearches.head.docOpens.isEmpty)
    assert(result.cardSearches.head.docOpens.isEmpty)
  }
}
