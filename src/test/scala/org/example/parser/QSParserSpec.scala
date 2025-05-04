package org.example.parser

import org.example.events.QS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QSParserSpec extends AnyFlatSpec with Matchers {

  private val validLine = "QS 2023-10-01T12:00:00 {search query} 12345 doc1 doc2"
  private val invalidPrefix = "INVALID 2023-10-01T12:00:00 {query} 12345"
  private val noBraces = "QS 2023-10-01T12:00:00 query 12345"
  private val emptyQuery = "QS 2023-10-01T12:00:00 {} 12345"
  private val invalidDate = "QS INVALID_DATE {query} 12345"
  private val noId = "QS 2023-10-01T12:00:00 {query}"

  "QSParser" should "parse valid QS line correctly" in {
    val result = QSParser.parse(validLine)
    result shouldBe defined
    result.get.query shouldBe "search query"
    result.get.id shouldBe "12345"
    result.get.docIds shouldBe List("doc1", "doc2")
  }

  it should "return None for line without QS prefix" in {
    QSParser.parse(invalidPrefix) shouldBe None
  }

  it should "return None if braces {} are missing" in {
    QSParser.parse(noBraces) shouldBe None
  }

  it should "return None for empty query" in {
    QSParser.parse(emptyQuery) shouldBe None
  }

  it should "return None if date is invalid" in {
    QSParser.parse(invalidDate) shouldBe None
  }

  it should "return None if ID is missing" in {
    QSParser.parse(noId) shouldBe None
  }
}