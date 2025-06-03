package org.example.processor.events

import org.example.processor.SessionBuilder
import org.example.processor.utils.ParseContext
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class CardSearchTest extends AnyFunSuite {

  test("parse valid card search") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 15.04.2020_02:51:51",
        "$134 ккт  при реализации товаров собственного производства через интернет-магазин",
        "CARD_SEARCH_END",
        "688324234 QSA_187707 PBI_253576 PBI_81154 QSA_92130 QSA_187458 QSA_171487 PBI_224661 PBI_202707 PBI_18358 QSA_185405 PBI_254644 PBI_271582 QSA_189745 QSA_181224 QSA_187148 PBI_198220 QSA_188617 PBI_252747 QSA_177101 PBI_272122 PBI_200505 PBI_236122 PBI_263070 QSA_128628 CMB_18859 PBI_252707 QSA_198375 PBI_253328 QSA_194812 PBI_252309 CJI_103839 QSA_125918 PBI_224804 PBI_259931 ACC_45615 QSA_120920 CJI_56831 QSA_199236 PBI_275273 PBI_215217 QSA_192920 QSA_186515 PBI_258870 PBI_255066 QSA_194225 PBI_236149 PBI_209831 PBI_271063 PBI_78826 CMB_18930 PBI_254280",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    CardSearch.parse(context)

    assert(context.currentSession.cardSearches.head.date.get.toString == "2020-04-15T02:51:51")
    assert(context.currentSession.cardSearches.head.searchId.contains(688324234))
    assert(
      context.currentSession.cardSearches.head.query == List(
        ("134", "ккт при реализации товаров собственного производства через интернет-магазин")
      )
    )
    assert(context.currentSession.cardSearches.head.docIds.head == "QSA_187707")
  }

  test("handle empty date") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START ",
        "$134 ккт  при реализации товаров собственного производства через интернет-магазин",
        "CARD_SEARCH_END",
        "688324234 QSA_187707 PBI_253576 PBI_81154 QSA_92130 QSA_187458 QSA_171487 PBI_224661 PBI_202707 PBI_18358 QSA_185405 PBI_254644 PBI_271582 QSA_189745 QSA_181224 QSA_187148 PBI_198220 QSA_188617 PBI_252747 QSA_177101 PBI_272122 PBI_200505 PBI_236122 PBI_263070 QSA_128628 CMB_18859 PBI_252707 QSA_198375 PBI_253328 QSA_194812 PBI_252309 CJI_103839 QSA_125918 PBI_224804 PBI_259931 ACC_45615 QSA_120920 CJI_56831 QSA_199236 PBI_275273 PBI_215217 QSA_192920 QSA_186515 PBI_258870 PBI_255066 QSA_194225 PBI_236149 PBI_209831 PBI_271063 PBI_78826 CMB_18930 PBI_254280",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    CardSearch.parse(context)

    assert(context.currentSession.cardSearches.head.date.isEmpty)
    assert(context.currentSession.cardSearches.head.searchId.contains(688324234))
    assert(
      context.currentSession.cardSearches.head.query == List(
        ("134", "ккт при реализации товаров собственного производства через интернет-магазин")
      )
    )
    assert(context.currentSession.cardSearches.head.docIds.head == "QSA_187707")
  }

  test("handle empty query") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 15.04.2020_02:51:51",
        "$",
        "CARD_SEARCH_END",
        "688324234 QSA_187707 PBI_253576 PBI_81154 QSA_92130 QSA_187458 QSA_171487 PBI_224661 PBI_202707 PBI_18358 QSA_185405 PBI_254644 PBI_271582 QSA_189745 QSA_181224 QSA_187148 PBI_198220 QSA_188617 PBI_252747 QSA_177101 PBI_272122 PBI_200505 PBI_236122 PBI_263070 QSA_128628 CMB_18859 PBI_252707 QSA_198375 PBI_253328 QSA_194812 PBI_252309 CJI_103839 QSA_125918 PBI_224804 PBI_259931 ACC_45615 QSA_120920 CJI_56831 QSA_199236 PBI_275273 PBI_215217 QSA_192920 QSA_186515 PBI_258870 PBI_255066 QSA_194225 PBI_236149 PBI_209831 PBI_271063 PBI_78826 CMB_18930 PBI_254280",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    CardSearch.parse(context)

    assert(context.currentSession.cardSearches.head.date.get.toString == "2020-04-15T02:51:51")
    assert(context.currentSession.cardSearches.head.searchId.contains(688324234))
    assert(
      context.currentSession.cardSearches.head.query == List()
    )
    assert(context.currentSession.cardSearches.head.docIds.head == "QSA_187707")
  }

  test("handle empty search ID") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 15.04.2020_02:51:51",
        "$134 ккт  при реализации товаров собственного производства через интернет-магазин",
        "CARD_SEARCH_END",
        " QSA_187707 PBI_253576 PBI_81154 QSA_92130 QSA_187458 QSA_171487 PBI_224661 PBI_202707 PBI_18358 QSA_185405 PBI_254644 PBI_271582 QSA_189745 QSA_181224 QSA_187148 PBI_198220 QSA_188617 PBI_252747 QSA_177101 PBI_272122 PBI_200505 PBI_236122 PBI_263070 QSA_128628 CMB_18859 PBI_252707 QSA_198375 PBI_253328 QSA_194812 PBI_252309 CJI_103839 QSA_125918 PBI_224804 PBI_259931 ACC_45615 QSA_120920 CJI_56831 QSA_199236 PBI_275273 PBI_215217 QSA_192920 QSA_186515 PBI_258870 PBI_255066 QSA_194225 PBI_236149 PBI_209831 PBI_271063 PBI_78826 CMB_18930 PBI_254280",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    CardSearch.parse(context)

    assert(context.currentSession.cardSearches.head.date.get.toString == "2020-04-15T02:51:51")
    assert(context.currentSession.cardSearches.head.searchId.isEmpty)
    assert(
      context.currentSession.cardSearches.head.query == List(
        ("134", "ккт при реализации товаров собственного производства через интернет-магазин")
      )
    )
    assert(context.currentSession.cardSearches.head.docIds.head == "QSA_187707")
  }

  test("handle negative search ID") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 15.04.2020_02:51:51",
        "$134 ккт  при реализации товаров собственного производства через интернет-магазин",
        "CARD_SEARCH_END",
        "-688324234 QSA_187707 PBI_253576 PBI_81154 QSA_92130 QSA_187458 QSA_171487 PBI_224661 PBI_202707 PBI_18358 QSA_185405 PBI_254644 PBI_271582 QSA_189745 QSA_181224 QSA_187148 PBI_198220 QSA_188617 PBI_252747 QSA_177101 PBI_272122 PBI_200505 PBI_236122 PBI_263070 QSA_128628 CMB_18859 PBI_252707 QSA_198375 PBI_253328 QSA_194812 PBI_252309 CJI_103839 QSA_125918 PBI_224804 PBI_259931 ACC_45615 QSA_120920 CJI_56831 QSA_199236 PBI_275273 PBI_215217 QSA_192920 QSA_186515 PBI_258870 PBI_255066 QSA_194225 PBI_236149 PBI_209831 PBI_271063 PBI_78826 CMB_18930 PBI_254280",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    CardSearch.parse(context)

    assert(context.currentSession.cardSearches.head.date.get.toString == "2020-04-15T02:51:51")
    assert(context.currentSession.cardSearches.head.searchId.contains(688324234))
    assert(
      context.currentSession.cardSearches.head.query == List(
        ("134", "ккт при реализации товаров собственного производства через интернет-магазин")
      )
    )
    assert(context.currentSession.cardSearches.head.docIds.head == "QSA_187707")
  }

  test("handle empty search result") {
    val filePath = "testFilePath"
    val rawSession = mutable
      .ArrayBuffer(
        "CARD_SEARCH_START 15.04.2020_02:51:51",
        "$134 ккт  при реализации товаров собственного производства через интернет-магазин",
        "CARD_SEARCH_END",
        "688324234",
        "SESSION_END 15.04.2020_02:53:55"
      )
      .iterator
      .buffered

    val currentSession = SessionBuilder(filePath)
    val context = ParseContext(filePath, rawSession, currentSession)
    CardSearch.parse(context)

    assert(context.currentSession.cardSearches.head.date.get.toString == "2020-04-15T02:51:51")
    assert(context.currentSession.cardSearches.head.searchId.contains(688324234))
    assert(
      context.currentSession.cardSearches.head.query == List(
        ("134", "ккт при реализации товаров собственного производства через интернет-магазин")
      )
    )
    assert(context.currentSession.cardSearches.head.docIds.isEmpty)
  }
}
