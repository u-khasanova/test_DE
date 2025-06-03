package org.example.processor.events

import org.example.processor.SessionBuilder
import org.example.processor.utils.{ParseContext, ParseError}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class QuickSearchTest extends AnyFunSuite {

  test("parse valid QS") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "QS 15.04.2020_02:49:12 {запросить в мвд результаты по заявлениям}",
        "234044353 LAW_340042 LAW_215903 LAW_169082 LAW_128707 LAW_354225 LAW_367400 LAW_347525 LAW_356744 LAW_302629 LAW_71929 LAW_339468 LAW_208423 LAW_339573 LAW_222988 LAW_217162 LAW_279342 LAW_356743 LAW_309416 PAP_44467 LAW_357280 LAW_368161 LAW_333442 LAW_284759 LAW_326846 LAW_347524 CMB_18303 CJI_108975 LAW_337161 LAW_347630 LAW_223044 LAW_339765 LAW_361228 LAW_321510 LAW_339346 LAW_360579 LAW_339343 LAW_349648 LAW_365649 LAW_222987 LAW_169079 LAW_360744 LAW_346719 LAW_317637 LAW_281757 LAW_281600 LAW_337160",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    QuickSearch.parse(context)

    assert(context.currentSession.quickSearches.head.date.get.toString == "2020-04-15T02:49:12")
    assert(context.currentSession.quickSearches.head.searchId.contains(234044353))
    assert(
      context.currentSession.quickSearches.head.query == "запросить в мвд результаты по заявлениям"
    )
    assert(context.currentSession.quickSearches.head.docIds.head == "LAW_340042")
  }

  test("handle empty date") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "QS  {запросить в мвд результаты по заявлениям}",
        "234044353 LAW_340042 LAW_215903 LAW_169082 LAW_128707 LAW_354225 LAW_367400 LAW_347525 LAW_356744 LAW_302629 LAW_71929 LAW_339468 LAW_208423 LAW_339573 LAW_222988 LAW_217162 LAW_279342 LAW_356743 LAW_309416 PAP_44467 LAW_357280 LAW_368161 LAW_333442 LAW_284759 LAW_326846 LAW_347524 CMB_18303 CJI_108975 LAW_337161 LAW_347630 LAW_223044 LAW_339765 LAW_361228 LAW_321510 LAW_339346 LAW_360579 LAW_339343 LAW_349648 LAW_365649 LAW_222987 LAW_169079 LAW_360744 LAW_346719 LAW_317637 LAW_281757 LAW_281600 LAW_337160",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    QuickSearch.parse(context)

    assert(context.currentSession.quickSearches.head.date.isEmpty)
    assert(context.currentSession.quickSearches.head.searchId.contains(234044353))
    assert(
      context.currentSession.quickSearches.head.query == "запросить в мвд результаты по заявлениям"
    )
    assert(context.currentSession.quickSearches.head.docIds.head == "LAW_340042")
    assert(
      context.errors == ListBuffer(
        ParseError(
          "testFilePath",
          "SESSION_END 15.04.2020_02:53:55",
          "DateTimeParser.process",
          "EmptyFieldError",
          "Field 'date' is empty or invalid"
        )
      )
    )
  }

  test("handle date secondary format") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "QS Wed,_15_Apr_2020_02:49:12_+0300 {запросить в мвд результаты по заявлениям}",
        "234044353 LAW_340042 LAW_215903 LAW_169082 LAW_128707 LAW_354225 LAW_367400 LAW_347525 LAW_356744 LAW_302629 LAW_71929 LAW_339468 LAW_208423 LAW_339573 LAW_222988 LAW_217162 LAW_279342 LAW_356743 LAW_309416 PAP_44467 LAW_357280 LAW_368161 LAW_333442 LAW_284759 LAW_326846 LAW_347524 CMB_18303 CJI_108975 LAW_337161 LAW_347630 LAW_223044 LAW_339765 LAW_361228 LAW_321510 LAW_339346 LAW_360579 LAW_339343 LAW_349648 LAW_365649 LAW_222987 LAW_169079 LAW_360744 LAW_346719 LAW_317637 LAW_281757 LAW_281600 LAW_337160",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    QuickSearch.parse(context)

    assert(context.currentSession.quickSearches.head.date.get.toString == "2020-04-15T02:49:12")
    assert(context.currentSession.quickSearches.head.searchId.contains(234044353))
    assert(
      context.currentSession.quickSearches.head.query == "запросить в мвд результаты по заявлениям"
    )
    assert(context.currentSession.quickSearches.head.docIds.head == "LAW_340042")
  }

  test("handle empty query") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "QS 15.04.2020_02:49:12 {}",
        "234044353 LAW_340042 LAW_215903 LAW_169082 LAW_128707 LAW_354225 LAW_367400 LAW_347525 LAW_356744 LAW_302629 LAW_71929 LAW_339468 LAW_208423 LAW_339573 LAW_222988 LAW_217162 LAW_279342 LAW_356743 LAW_309416 PAP_44467 LAW_357280 LAW_368161 LAW_333442 LAW_284759 LAW_326846 LAW_347524 CMB_18303 CJI_108975 LAW_337161 LAW_347630 LAW_223044 LAW_339765 LAW_361228 LAW_321510 LAW_339346 LAW_360579 LAW_339343 LAW_349648 LAW_365649 LAW_222987 LAW_169079 LAW_360744 LAW_346719 LAW_317637 LAW_281757 LAW_281600 LAW_337160",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    QuickSearch.parse(context)

    assert(context.currentSession.quickSearches.head.date.get.toString == "2020-04-15T02:49:12")
    assert(context.currentSession.quickSearches.head.searchId.contains(234044353))
    assert(context.currentSession.quickSearches.head.query.isEmpty)
    assert(context.currentSession.quickSearches.head.docIds.head == "LAW_340042")
  }

  test("handle empty search ID") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "QS 15.04.2020_02:49:12 {запросить в мвд результаты по заявлениям}",
        " LAW_340042 LAW_215903 LAW_169082 LAW_128707 LAW_354225 LAW_367400 LAW_347525 LAW_356744 LAW_302629 LAW_71929 LAW_339468 LAW_208423 LAW_339573 LAW_222988 LAW_217162 LAW_279342 LAW_356743 LAW_309416 PAP_44467 LAW_357280 LAW_368161 LAW_333442 LAW_284759 LAW_326846 LAW_347524 CMB_18303 CJI_108975 LAW_337161 LAW_347630 LAW_223044 LAW_339765 LAW_361228 LAW_321510 LAW_339346 LAW_360579 LAW_339343 LAW_349648 LAW_365649 LAW_222987 LAW_169079 LAW_360744 LAW_346719 LAW_317637 LAW_281757 LAW_281600 LAW_337160",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    QuickSearch.parse(context)

    assert(context.currentSession.quickSearches.head.date.get.toString == "2020-04-15T02:49:12")
    assert(context.currentSession.quickSearches.head.searchId.isEmpty)
    assert(
      context.currentSession.quickSearches.head.query == "запросить в мвд результаты по заявлениям"
    )
    assert(context.currentSession.quickSearches.head.docIds.head == "LAW_340042")
  }

  test("handle negative search ID") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "QS 15.04.2020_02:49:12 {запросить в мвд результаты по заявлениям}",
        "-234044353 LAW_340042 LAW_215903 LAW_169082 LAW_128707 LAW_354225 LAW_367400 LAW_347525 LAW_356744 LAW_302629 LAW_71929 LAW_339468 LAW_208423 LAW_339573 LAW_222988 LAW_217162 LAW_279342 LAW_356743 LAW_309416 PAP_44467 LAW_357280 LAW_368161 LAW_333442 LAW_284759 LAW_326846 LAW_347524 CMB_18303 CJI_108975 LAW_337161 LAW_347630 LAW_223044 LAW_339765 LAW_361228 LAW_321510 LAW_339346 LAW_360579 LAW_339343 LAW_349648 LAW_365649 LAW_222987 LAW_169079 LAW_360744 LAW_346719 LAW_317637 LAW_281757 LAW_281600 LAW_337160",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    QuickSearch.parse(context)

    assert(context.currentSession.quickSearches.head.date.get.toString == "2020-04-15T02:49:12")
    assert(context.currentSession.quickSearches.head.searchId.contains(234044353))
    assert(
      context.currentSession.quickSearches.head.query == "запросить в мвд результаты по заявлениям"
    )
    assert(context.currentSession.quickSearches.head.docIds.head == "LAW_340042")
  }

  test("handle empty search result") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "QS 15.04.2020_02:49:12 {запросить в мвд результаты по заявлениям}",
        "234044353 ",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    QuickSearch.parse(context)

    assert(context.currentSession.quickSearches.head.date.get.toString == "2020-04-15T02:49:12")
    assert(context.currentSession.quickSearches.head.searchId.contains(234044353))
    assert(
      context.currentSession.quickSearches.head.query == "запросить в мвд результаты по заявлениям"
    )
    assert(context.currentSession.quickSearches.head.docIds.isEmpty)
  }
}
