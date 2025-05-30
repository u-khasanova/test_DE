package org.example.events

import org.example.errorProcessors.EmptyFieldLogger
import org.example.processors.DateTimeProcessor
import org.example.processors.RawDataProcessor.ParseContext

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

    val datePart = line(0)

    val date = DateTimeProcessor.process(datePart)

    val queryRaw =
      if (line.tail(0).startsWith("{")) line.tail.mkString(" ")
      else line.mkString(" ")

    val query = queryRaw.slice(1, queryRaw.length - 1)

    val searchId = Try(nextLine(0).toInt.abs).toOption

    val docIds = if (searchId.isEmpty) nextLine.toList else nextLine.tail.toList

    Seq(("date", date), ("searchId", searchId))
      .foreach { case (name, value) =>
        EmptyFieldLogger.log(context, name, value, line.mkString(" "), "QuickSearch.parse")
      }

    context.currentSession.quickSearches += QuickSearch(
      date,
      searchId,
      query,
      docIds
    )
  }
}
