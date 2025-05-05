package org.example.events

import org.example.parser.DateTimeParts
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CardSearchSpec extends AnyFlatSpec with Matchers {

  private val validInput =
    """CARD_SEARCH_START 20_Mar_2020_11:45:25 $134 3-ндфл вычет на лечение $0 PBI_248726 CARD_SEARCH_END 1083982343 PBI_248726 DOC_123""".stripMargin

  "CardSearch.parse()" should "correctly parse valid input" in {
    val result = CardSearch.parse(validInput)
    result shouldBe defined
    val cardSearch = result.get
    cardSearch.date shouldBe DateTimeParts("20.03.2020","11:45:25")
    cardSearch.id shouldBe "1083982343"
    cardSearch.query should include("3-ндфл вычет на лечение")
    cardSearch.docIds should contain allOf("PBI_248726", "DOC_123")
  }

  it should "return None when date is invalid" in {
    val invalidDateInput =
      """CARD_SEARCH_START INVALID_DATE
        |query text
        |CARD_SEARCH_END
        |123""".stripMargin
    CardSearch.parse(invalidDateInput) shouldBe None
  }

  it should "handle missing query" in {
    val noQueryInput =
      """CARD_SEARCH_START 20_Mar_2020_11:45:25
        |CARD_SEARCH_END
        |123""".stripMargin
    val result = CardSearch.parse(noQueryInput)
    result shouldBe defined
    result.get.query shouldBe empty
  }

  it should "handle missing docIds" in {
    val noDocIdsInput =
      """CARD_SEARCH_START 20_Mar_2020_11:45:25
        |query
        |CARD_SEARCH_END
        |123""".stripMargin
    val result = CardSearch.parse(noDocIdsInput)
    result shouldBe defined
    result.get.docIds shouldBe empty
  }

  it should "handle missing id" in {
    val noIdInput =
      """CARD_SEARCH_START 20_Mar_2020_11:45:25
        |query
        |CARD_SEARCH_END
        |""".stripMargin
    CardSearch.parse(noIdInput) shouldBe None
  }

  it should "handle malformed CARD_SEARCH_END section" in {
    val malformedEndInput =
      """CARD_SEARCH_START 20_Mar_2020_11:45:25
        |query
        |CARD_SEARCH_END""".stripMargin
    CardSearch.parse(malformedEndInput) shouldBe None
  }

  it should "handle empty input" in {
    CardSearch.parse("") shouldBe None
  }

  it should "handle input without CARD_SEARCH_END" in {
    val noEndInput =
      """CARD_SEARCH_START 20_Mar_2020_11:45:25
        |query""".stripMargin
    CardSearch.parse(noEndInput) shouldBe None
  }

  it should "handle negative id" in {
    val negativeIdInput =
      """CARD_SEARCH_START 20_Mar_2020_11:45:25
        |query
        |CARD_SEARCH_END
        |-1083982343""".stripMargin
    val result = CardSearch.parse(negativeIdInput)
    result shouldBe defined
    result.get.id shouldBe "1083982343"
  }
}