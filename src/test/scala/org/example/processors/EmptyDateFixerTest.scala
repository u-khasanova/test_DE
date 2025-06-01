package org.example.processors

import org.example.processor.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.processor.fixers.EmptyDateFixer
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class EmptyDateFixerTest extends AnyFunSuite {
  private val testDate = LocalDateTime.parse(
    "01.09.2020_06:44:35",
    DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  )
  private val testDate2 = LocalDateTime.parse(
    "01.09.2020_06:51:02",
    DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  )

  test("recover should fill empty dates in all components") {
    val session = Session(
      "file.txt",
      Some(testDate),
      None,
      List(
        QuickSearch(None, None, "query1", List.empty),
        QuickSearch(Some(testDate2), None, "query2", List.empty)
      ),
      List(
        CardSearch(None, None, "card1", List.empty)
      ),
      List(
        DocOpen(None, None, None),
        DocOpen(Some(testDate2), None, None)
      )
    )

    val recovered = EmptyDateFixer.recover(session)

    assert(recovered.quickSearches.head.date.contains(testDate))
    assert(recovered.quickSearches(1).date.contains(testDate2))
    assert(recovered.cardSearches.head.date.contains(testDate))
    assert(recovered.docOpens.head.date.contains(testDate))
    assert(recovered.docOpens(1).date.contains(testDate2))
  }
}
