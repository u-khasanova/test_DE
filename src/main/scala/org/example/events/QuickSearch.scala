package org.example.events

import org.example.processors.RawDataProcessor.ParseContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.collection.mutable
import scala.util.{Success, Try}

case class QuickSearch(
    date: Option[LocalDateTime],
    id: Option[Int],
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

    val dateFormats = Array(
      DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss"),
      DateTimeFormatter.ofPattern("EEE,_dd_MMM_yyyy_HH:mm:ss_XXXX", Locale.US)
    )

    val date = dateFormats
      .map { formatter =>
        Try(LocalDateTime.parse(datePart, formatter))
      }
      .collectFirst { case Success(date) =>
        date
      }

    val query =
      if (line.tail(0).startsWith("{")) line.tail.mkString(" ")
      else line.mkString(" ")

    val id = Try(nextLine(0).toInt.abs).toOption

    val docIds = if (id.isEmpty) nextLine.toList else nextLine.tail.toList

    context.currentSession.quickSearches += QuickSearch(
      date,
      id,
      query.slice(1, query.length - 1),
      docIds
    )
  }
}
