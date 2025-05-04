package org.example

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DummySpec extends AnyFlatSpec with Matchers {  // Важно: class, не object!
  "Тест 1" should "работать" in {
    1 + 1 shouldBe 2
  }

  "Тест 2" should "проверять исключения" in {
    a [NoSuchElementException] should be thrownBy {
      List.empty.head
    }
  }
}