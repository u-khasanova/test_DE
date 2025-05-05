package org.example.events

import org.example.parser.DateTimeParts
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QSSpec extends AnyFlatSpec with Matchers {

  private val validInput = "QS 20_Mar_2020_11:47:37 {договор поставки газа} 258306984 CJI_125431 CJI_95422"

  "QS.parse()" should "correctly parse valid input" in {
    val result = QS.parse(validInput)
    result shouldBe defined
    val qs = result.get
    qs.date shouldBe Some(DateTimeParts("20.03.2020", "11:47:37"))
    qs.id shouldBe "258306984"
    qs.query shouldBe "договор поставки газа"
    qs.docIds should contain allOf("CJI_125431", "CJI_95422")
  }

  it should "return None when missing QS prefix" in {
    QS.parse("INVALID 20_Mar_2020_11:47:37 {query} 123") shouldBe None
  }

  it should "handle empty query" in {
    val input = "QS 20_Mar_2020_11:47:37 {} 258306984"
    val result = QS.parse(input)
    result shouldBe defined
    result.get.query shouldBe empty
  }

  it should "handle missing docIds" in {
    val input = "QS 20_Mar_2020_11:47:37 {query} 258306984"
    val result = QS.parse(input)
    result shouldBe defined
    result.get.docIds shouldBe empty
  }

  it should "return None when missing braces" in {
    QS.parse("QS 20_Mar_2020_11:47:37 query 258306984") shouldBe None
  }

  it should "return None for invalid date" in {
    val input = "QS INVALID_DATE {query} 258306984"
    QS.parse(input) shouldBe None
  }

  it should "return None when missing id" in {
    val input = "QS 20_Mar_2020_11:47:37 {query}"
    QS.parse(input) shouldBe None
  }

  it should "handle multi-word query" in {
    val input = "QS 20_Mar_2020_11:47:37 {договор поставки газа} 258306984"
    val result = QS.parse(input)
    result shouldBe defined
    result.get.query shouldBe "договор поставки газа"
  }
}