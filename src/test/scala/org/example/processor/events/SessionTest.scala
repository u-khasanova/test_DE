package org.example.processor.events

import org.example.processor.SessionBuilder
import org.example.processor.utils.{ParseContext, ParseError}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SessionTest extends AnyFunSuite {

  test("parse valid session") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "SESSION_START 15.04.2020_02:47:22",
        "QS 15.04.2020_02:49:12 {запросить в мвд результаты по заявлениям}",
        "234044353 LAW_340042 LAW_215903 LAW_169082 LAW_128707 LAW_354225 LAW_367400 LAW_347525 LAW_356744 LAW_302629 LAW_71929 LAW_339468 LAW_208423 LAW_339573 LAW_222988 LAW_217162 LAW_279342 LAW_356743 LAW_309416 PAP_44467 LAW_357280 LAW_368161 LAW_333442 LAW_284759 LAW_326846 LAW_347524 CMB_18303 CJI_108975 LAW_337161 LAW_347630 LAW_223044 LAW_339765 LAW_361228 LAW_321510 LAW_339346 LAW_360579 LAW_339343 LAW_349648 LAW_365649 LAW_222987 LAW_169079 LAW_360744 LAW_346719 LAW_317637 LAW_281757 LAW_281600 LAW_337160",
        "DOC_OPEN 15.04.2020_02:49:51 234044353 LAW_367400",
        "DOC_OPEN 15.04.2020_02:51:03 234044353 LAW_346719",
        "DOC_OPEN 15.04.2020_02:51:16 234044353 LAW_317637",
        "DOC_OPEN 15.04.2020_02:51:17 234044353 LAW_339468",
        "CARD_SEARCH_START 15.04.2020_02:51:51",
        "$134 ккт  при реализации товаров собственного производства через интернет-магазин",
        "CARD_SEARCH_END",
        "688324234 QSA_187707 PBI_253576 PBI_81154 QSA_92130 QSA_187458 QSA_171487 PBI_224661 PBI_202707 PBI_18358 QSA_185405 PBI_254644 PBI_271582 QSA_189745 QSA_181224 QSA_187148 PBI_198220 QSA_188617 PBI_252747 QSA_177101 PBI_272122 PBI_200505 PBI_236122 PBI_263070 QSA_128628 CMB_18859 PBI_252707 QSA_198375 PBI_253328 QSA_194812 PBI_252309 CJI_103839 QSA_125918 PBI_224804 PBI_259931 ACC_45615 QSA_120920 CJI_56831 QSA_199236 PBI_275273 PBI_215217 QSA_192920 QSA_186515 PBI_258870 PBI_255066 QSA_194225 PBI_236149 PBI_209831 PBI_271063 PBI_78826 CMB_18930 PBI_254280",
        "DOC_OPEN 15.04.2020_02:52:00 688324234 PBI_18358",
        "DOC_OPEN 15.04.2020_02:53:54 234044353 CJI_108975",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)

    val context = ParseContext(filePath, rawSession, currentSession)

    val session = Session.parse(context)

    assert(session.isInstanceOf[Session])
    assert(session.startDate.get.toString == "2020-04-15T02:47:22")
    assert(session.endDate.get.toString == "2020-04-15T02:53:55")

    assert(session.quickSearches.head.date.get.toString == "2020-04-15T02:49:12")
    assert(session.quickSearches.head.searchId.contains(234044353))
    assert(session.quickSearches.head.query == "запросить в мвд результаты по заявлениям")
    assert(session.quickSearches.head.docIds.head == "LAW_340042")

    assert(session.cardSearches.head.date.get.toString == "2020-04-15T02:51:51")
    assert(session.cardSearches.head.searchId.contains(688324234))
    assert(
      session.cardSearches.head.query == List(
        ("134", "ккт при реализации товаров собственного производства через интернет-магазин")
      )
    )
    assert(session.cardSearches.head.docIds.head == "QSA_187707")

    assert(session.docOpens.head.date.get.toString == "2020-04-15T02:49:51")
    assert(session.docOpens.head.searchId.contains(234044353))
    assert(session.docOpens.head.docId.contains("LAW_367400"))
  }

  test("parse session with invalid dates") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "SESSION_START invalid_date",
        "SESSION_END invalid_date"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    val session = Session.parse(context)

    assert(session.isInstanceOf[Session])
    assert(session.startDate.isEmpty)
    assert(session.endDate.isEmpty)
    assert(
      context.errors == ListBuffer(
        ParseError(
          "testFilePath",
          "SESSION_END invalid_date",
          "DateTimeParser.process",
          "EmptyFieldError",
          "Field 'date' is empty or invalid"
        ),
        ParseError(
          "testFilePath",
          "[END OF FILE]",
          "DateTimeParser.process",
          "EmptyFieldError",
          "Field 'date' is empty or invalid"
        )
      )
    )
  }

  test("warn about trailing line after session end") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "SESSION_START 15.04.2020_02:47:22",
        "SESSION_END 15.04.2020_02:53:55",
        "some line"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    Session.parse(context)

    assert(
      context.errors == ListBuffer(
        ParseError(
          "testFilePath",
          "some line",
          "Session.parse",
          "TrailingLineAfterSessionEndError",
          "Unexpected line after SESSION_END"
        )
      )
    )
  }
}
