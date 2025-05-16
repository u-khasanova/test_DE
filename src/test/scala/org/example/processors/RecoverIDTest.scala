package org.example.processors

import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.parser.DateTime
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RecoverIDTest extends AnyFunSuite with Matchers {
  private val testDate = DateTime.parse("08.11.2020_12:29:47")

  test("recover DocOpen ids from unique QuickSearch references") {
    val qs = QuickSearch(testDate, 123, "query", List("doc1", "doc2"))
    val docOpen1 = DocOpen(testDate, 0, "doc1") // Should be recovered
    val docOpen2 = DocOpen(testDate, 0, "doc2") // Should be recovered
    val docOpen3 = DocOpen(testDate, 456, "doc3") // Should remain unchanged

    val session = Session(
      testDate,
      testDate,
      List(qs),
      List.empty,
      List(docOpen1, docOpen2, docOpen3)
    )

    val recovered = RecoverID.recoverIds(session)

    recovered.docOpens.find(_.docId == "doc1").get.id shouldBe 123
    recovered.docOpens.find(_.docId == "doc2").get.id shouldBe 123
    recovered.docOpens.find(_.docId == "doc3").get.id shouldBe 456
  }

  test("don't recover ambiguous DocOpen ids") {
    val qs1 = QuickSearch(testDate, 123, "query1", List("doc1"))
    val qs2 = QuickSearch(testDate, 456, "query2", List("doc1")) // Same docId
    val docOpen = DocOpen(testDate, 0, "doc1") // Should NOT be recovered

    val session = Session(
      testDate,
      testDate,
      List(qs1, qs2),
      List.empty,
      List(docOpen)
    )

    val recovered = RecoverID.recoverIds(session)
    recovered.docOpens.head.id shouldBe 0
  }

  test("recover QuickSearch id from DocOpens") {
    val docOpen = DocOpen(testDate, 123, "doc1")
    val qs =
      QuickSearch(testDate, 0, "query", List("doc1")) // Should be recovered

    val session = Session(
      testDate,
      testDate,
      List(qs),
      List.empty,
      List(docOpen)
    )

    val recovered = RecoverID.recoverIds(session)
    recovered.quickSearches.head.id shouldBe 123
  }

  test("recover CardSearch id from DocOpens") {
    val docOpen = DocOpen(testDate, 456, "doc2")
    val cs =
      CardSearch(testDate, 0, "query", List("doc2")) // Should be recovered

    val session = Session(
      testDate,
      testDate,
      List.empty,
      List(cs),
      List(docOpen)
    )

    val recovered = RecoverID.recoverIds(session)
    recovered.cardSearches.head.id shouldBe 456
  }

  test("handle empty session") {
    val session =
      Session(testDate, testDate, List.empty, List.empty, List.empty)
    val recovered = RecoverID.recoverIds(session)
    recovered shouldBe session
  }

  test("don't recover ids when no matches found") {
    val qs = QuickSearch(testDate, 0, "query", List("doc1"))
    val cs = CardSearch(testDate, 0, "query", List("doc2"))
    val docOpen = DocOpen(testDate, 0, "doc3")

    val session = Session(
      testDate,
      testDate,
      List(qs),
      List(cs),
      List(docOpen)
    )

    val recovered = RecoverID.recoverIds(session)
    recovered.quickSearches.head.id shouldBe 0
    recovered.cardSearches.head.id shouldBe 0
    recovered.docOpens.head.id shouldBe 0
  }
}
