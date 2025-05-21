package org.example.processors

import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.fields.DateTime
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RecoverIDTest extends AnyFunSuite with Matchers {
  private val testDate = DateTime.parse("08.11.2020_12:29:47")

  test("recover DocOpen ids from unique QuickSearch references") {
    val qs = QuickSearch(testDate, Some(123), "query", List("doc1", "doc2"))
    val docOpen1 = DocOpen(testDate, Some(0), Some("doc1"))
    val docOpen2 = DocOpen(testDate, Some(0), Some("doc2"))
    val docOpen3 = DocOpen(testDate, Some(456), Some("doc3"))

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List.empty,
      List(docOpen1, docOpen2, docOpen3)
    )

    val recovered = RecoverID.recover(session)

    recovered.docOpens.find(_.docId.contains("doc1")).get.id shouldBe 123
    recovered.docOpens.find(_.docId.contains("doc2")).get.id shouldBe 123
    recovered.docOpens.find(_.docId.contains("doc3")).get.id shouldBe 456
  }

  test("don't recover ambiguous DocOpen ids") {
    val qs1 = QuickSearch(testDate, Some(123), "query1", List("doc1"))
    val qs2 =
      QuickSearch(testDate, Some(456), "query2", List("doc1")) // Same docId
    val docOpen =
      DocOpen(testDate, Some(0), Some("doc1")) // Should NOT be recovered

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs1, qs2),
      List.empty,
      List(docOpen)
    )

    val recovered = RecoverID.recover(session)
    recovered.docOpens.head.id shouldBe 0
  }

  test("recover QuickSearch id from DocOpens") {
    val docOpen = DocOpen(testDate, Some(123), Some("doc1"))
    val qs =
      QuickSearch(
        testDate,
        Some(0),
        "query",
        List("doc1")
      ) // Should be recovered

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List.empty,
      List(docOpen)
    )

    val recovered = RecoverID.recover(session)
    recovered.quickSearches.head.id shouldBe 123
  }

  test("recover CardSearch id from DocOpens") {
    val docOpen = DocOpen(testDate, Some(456), Some("doc2"))
    val cs =
      CardSearch(
        testDate,
        Some(0),
        "query",
        List("doc2")
      ) // Should be recovered

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List.empty,
      List(cs),
      List(docOpen)
    )

    val recovered = RecoverID.recover(session)
    recovered.cardSearches.head.id shouldBe 456
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
    val recovered = RecoverID.recover(session)
    recovered shouldBe session
  }

  test("don't recover ids when no matches found") {
    val qs = QuickSearch(testDate, Some(0), "query", List("doc1"))
    val cs = CardSearch(testDate, Some(0), "query", List("doc2"))
    val docOpen = DocOpen(testDate, Some(0), Some("doc3"))

    val session = Session(
      "file.txt",
      testDate,
      testDate,
      List(qs),
      List(cs),
      List(docOpen)
    )

    val recovered = RecoverID.recover(session)
    recovered.quickSearches.head.id shouldBe 0
    recovered.cardSearches.head.id shouldBe 0
    recovered.docOpens.head.id shouldBe 0
  }
}
