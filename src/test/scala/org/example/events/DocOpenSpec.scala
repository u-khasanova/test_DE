package org.example.events

import org.example.parser.DateTimeParts
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DocOpenSpec extends AnyFlatSpec with Matchers {

  private val validInput = "DOC_OPEN 20_Mar_2020_11:46:27 258306984 CJI_102674"

  "DocOpen.parse()" should "correctly parse valid input" in {
    val result = DocOpen.parse(validInput)
    result shouldBe defined
    val doc = result.get
    doc.date shouldBe DateTimeParts("20.03.2020", "11:46:27")
    doc.id shouldBe "258306984"
    doc.docId shouldBe "CJI_102674"
  }

  it should "return None for invalid date" in {
    val input = "DOC_OPEN INVALID_DATE 258306984 CJI_102674"
    DocOpen.parse(input) shouldBe None
  }

  it should "return None when missing id" in {
    val input = "DOC_OPEN 20_Mar_2020_11:46:27"
    DocOpen.parse(input) shouldBe None
  }

  it should "return None when missing docId" in {
    val input = "DOC_OPEN 20_Mar_2020_11:46:27 258306984"
    DocOpen.parse(input) shouldBe None
  }

  it should "handle numeric docId" in {
    val input = "DOC_OPEN 20_Mar_2020_11:46:27 258306984 12345"
    val result = DocOpen.parse(input)
    result shouldBe defined
    result.get.docId shouldBe "12345"
  }

  it should "handle special characters in docId" in {
    val input = "DOC_OPEN 20_Mar_2020_11:46:27 258306984 ABC-123_XYZ"
    val result = DocOpen.parse(input)
    result shouldBe defined
    result.get.docId shouldBe "ABC-123_XYZ"
  }

  it should "clean id from non-digit characters" in {
    val input = "DOC_OPEN 20_Mar_2020_11:46:27 -258306984- CJI_102674"
    val result = DocOpen.parse(input)
    result shouldBe defined
    result.get.id shouldBe "258306984"
  }
}