package org.example.processor.events

import org.example.processor.DateTimeParser
import org.example.processor.utils.ParseContext

import java.time.LocalDateTime
import scala.collection.mutable
import scala.util.Try

case class QuickSearch(
    var date: Option[LocalDateTime],
    var searchId: Option[Int],
    query: String,
    docIds: List[String],
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
)

object QuickSearch {

  def parse(
      context: ParseContext
  ): Unit = {

    val line = context.iterator
      .next()
      .split("\\s+", 3)
      .tail

    val nextLine = context.iterator
      .next()
      .split("\\s+")

    val date = DateTimeParser.process(context, line(0))

    val queryRaw =
      if (line.tail(0).startsWith("{")) line.tail.mkString(" ")
      else line.mkString(" ")

    val query = queryRaw.substring(1, queryRaw.length - 1)

    if (query.isEmpty) context.addWarning("QuickSearch.parse", "query")

    val searchId = Try(nextLine(0).toInt.abs).toOption

    if (searchId.isEmpty) context.addWarning("QuickSearch.parse", "searchId")

    val docIds = if (searchId.isEmpty) nextLine.toList else nextLine.tail.toList

    if (docIds.isEmpty) context.addWarning("QuickSearch.parse", "docIds")

    context.currentSession.quickSearches += QuickSearch(
      date,
      searchId,
      query,
      docIds
    )
  }
}
