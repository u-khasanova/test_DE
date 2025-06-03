package org.example.processor

import org.example.processor.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.processor.fixers.EmptyIdFixer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class EmptyIdFixerTest extends AnyFunSuite with Matchers {
  private val testDate = Some(
    LocalDateTime.parse(
      "08.11.2020_12:29:47",
      DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
    )
  )

  test("recover DocOpen ids from unique QuickSearch references") {
    val qs = QuickSearch(testDate, Some(123), "query", List("doc1", "doc2"))
    val docOpen1 = DocOpen(testDate, None, Some("doc1"))
    val docOpen2 = DocOpen(testDate, None, Some("doc2"))
    val docOpen3 = DocOpen(testDate, None, Some("doc3"))

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List.empty,
      List(docOpen1, docOpen2, docOpen3)
    )

    val recovered = EmptyIdFixer.recover(session)

    assert(
      recovered.docOpens.find(_.docId.contains("doc1")).get.searchId.contains(123)
    )
    assert(
      recovered.docOpens.find(_.docId.contains("doc2")).get.searchId.contains(123)
    )
    assert(
      recovered.docOpens.find(_.docId.contains("doc3")).get.searchId.isEmpty
    )
  }

  test(
    "recover DocOpen ids from unique QuickSearch and CardSearch references"
  ) {
    val qs = QuickSearch(testDate, Some(123), "query", List("doc1", "doc2"))
    val cs = CardSearch(testDate, Some(456), List(("key", "value")), List("doc3"))
    val docOpen1 = DocOpen(testDate, None, Some("doc1"))
    val docOpen2 = DocOpen(testDate, None, Some("doc2"))
    val docOpen3 = DocOpen(testDate, None, Some("doc3"))

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List(cs),
      List(docOpen1, docOpen2, docOpen3)
    )

    val recovered = EmptyIdFixer.recover(session)

    assert(
      recovered.docOpens.find(_.docId.contains("doc1")).get.searchId.contains(123)
    )
    assert(
      recovered.docOpens.find(_.docId.contains("doc2")).get.searchId.contains(123)
    )
    assert(
      recovered.docOpens.find(_.docId.contains("doc3")).get.searchId.contains(456)
    )
  }

  test("don't recover ambiguous DocOpen ids test 1") {
    val qs1 = QuickSearch(testDate, Some(123), "query1", List("doc1"))
    val qs2 =
      QuickSearch(testDate, Some(456), "query2", List("doc1")) // Same docId
    val docOpen =
      DocOpen(testDate, None, Some("doc1")) // Should NOT be recovered

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs1, qs2),
      List.empty,
      List(docOpen)
    )

    val recovered = EmptyIdFixer.recover(session)
    assert(
      recovered.docOpens.find(_.docId.contains("doc1")).get.searchId.isEmpty
    )
  }

  test("don't recover ambiguous DocOpen ids test 2") {
    val qs = QuickSearch(testDate, Some(123), "query1", List("doc1"))
    val cs =
      CardSearch(testDate, Some(456), List(("key", "value")), List("doc1")) // Same docId
    val docOpen =
      DocOpen(testDate, None, Some("doc1")) // Should NOT be recovered

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List(cs),
      List(docOpen)
    )

    val recovered = EmptyIdFixer.recover(session)
    assert(
      recovered.docOpens.find(_.docId.contains("doc1")).get.searchId.isEmpty
    )
  }

  test("recover QuickSearch id from DocOpens") {
    val docOpen = DocOpen(testDate, Some(123), Some("doc1"))
    val qs =
      QuickSearch(
        testDate,
        None,
        "query",
        List("doc1")
      )

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List.empty,
      List(docOpen)
    )

    val recovered = EmptyIdFixer.recover(session)
    assert(recovered.quickSearches.head.searchId.contains(123))
  }

  test("recover CardSearch id from DocOpens") {
    val docOpen = DocOpen(testDate, Some(456), Some("doc2"))
    val cs =
      CardSearch(
        testDate,
        None,
        List(("key", "value")),
        List("doc2")
      )

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List.empty,
      List(cs),
      List(docOpen)
    )

    val recovered = EmptyIdFixer.recover(session)
    assert(recovered.cardSearches.head.searchId.contains(456))
  }

  test("don't recover ambiguous CardSearch or QuickSearch id") {
    val docOpen = DocOpen(testDate, Some(456), Some("doc2"))
    val qs =
      QuickSearch(
        testDate,
        None,
        "query",
        List("doc2")
      )
    val cs =
      CardSearch(
        testDate,
        None,
        List(("key", "value")),
        List("doc2")
      )

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List(cs),
      List(docOpen)
    )

    val recovered = EmptyIdFixer.recover(session)
    assert(recovered.quickSearches.head.searchId.isEmpty)
    assert(recovered.cardSearches.head.searchId.isEmpty)
    assert(
      recovered.docOpens.find(_.docId.contains("doc2")).get.searchId.contains(456)
    )
  }

  test("handle empty session") {
    val session =
      Session(
        "file.txt",
        testDate,
        testDate,
        List.empty,
        List.empty,
        List.empty
      )
    val recovered = EmptyIdFixer.recover(session)
    recovered shouldBe session
  }

  test("don't recover ids when no matches found") {
    val qs = QuickSearch(testDate, Some(123), "query", List("doc1"))
    val cs = CardSearch(testDate, Some(456), List(("key", "value")), List("doc2"))
    val docOpen = DocOpen(testDate, None, Some("doc3"))

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List(cs),
      List(docOpen)
    )

    val recovered = EmptyIdFixer.recover(session)
    assert(recovered.quickSearches.head.searchId.contains(123))
    assert(recovered.cardSearches.head.searchId.contains(456))
    assert(recovered.docOpens.head.searchId.isEmpty)
  }
}
