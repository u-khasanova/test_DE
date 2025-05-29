package org.example.events

import org.example.processors.RawDataProcessor.ParseContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.collection.mutable
import scala.util.{Success, Try}

case class CardSearch(
    date: Option[LocalDateTime],
    id: Option[Int],
    query: String,
    docIds: List[String],
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
)

object CardSearch {

  def parse(
      context: ParseContext
  ): Unit = {
    val content = new mutable.StringBuilder(context.iterator.next().trim)
    var foundEnd = false

    while (context.iterator.hasNext && !foundEnd) {
      val nextLine = context.iterator.next()
      content.append(s" $nextLine")
      foundEnd = nextLine.trim.startsWith("CARD_SEARCH_END")
      if (foundEnd) content.append(s" ${context.iterator.next()}")
    }

    val fullContent = content.toString()

    val parts = fullContent.split("CARD_SEARCH_END")
    val beforeEnd = parts(0).trim.split("\\s+").tail
    val afterEnd = parts(1).trim.split("\\s+")

    val datePart = beforeEnd(0)

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
      if (date.isEmpty) beforeEnd.mkString(" ")
      else beforeEnd.tail.mkString(" ")
    val id = Try(afterEnd.head.toInt.abs).toOption
    val docIds = if (id.isEmpty) afterEnd.toList else afterEnd.tail.toList

    context.currentSession.cardSearches += CardSearch(date, id, query, docIds)
  }
}
