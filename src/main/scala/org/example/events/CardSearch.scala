package org.example.events

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class CardSearch(
    date: Option[LocalDateTime],
    id: Option[Int],
    query: String,
    docIds: List[String],
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
)

object CardSearch {

  def parse(
      lines: BufferedIterator[String]
  ): CardSearch = {
    val content = new mutable.StringBuilder(lines.next().trim)
    var foundEnd = false

    while (lines.hasNext && !foundEnd) {
      val nextLine = lines.next()
      content.append(s" $nextLine")
      foundEnd = nextLine.trim.startsWith("CARD_SEARCH_END")
      if (foundEnd) content.append(s" ${lines.next()}")
    }

    val fullContent = content.toString()

    val parts = fullContent.split("CARD_SEARCH_END")
    val beforeEnd = parts(0).trim.split("\\s+").tail
    val afterEnd = parts(1).trim.split("\\s+")

    val datePart = beforeEnd(0)

    val date = Try(
      LocalDateTime
        .parse(datePart, DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss"))
    ) match {
      case Success(date) =>
        Some(date)
      case Failure(e) =>
        Try(
          LocalDateTime.parse(
            datePart.split("_").slice(1, 5).mkString("_"),
            DateTimeFormatter.ofPattern("dd_MMM_yyyy_HH:mm:ss", Locale.US)
          )
        ) match {
          case Success(date) =>
            Some(date)
          case Failure(e) => None
        }
    }

    val query =
      if (date.isEmpty) beforeEnd.mkString(" ")
      else beforeEnd.tail.mkString(" ")
    val id = Try(afterEnd.head.toInt.abs).toOption
    val docIds = if (id.isEmpty) afterEnd.toList else afterEnd.tail.toList

    CardSearch(date, id, query, docIds)
  }
}
