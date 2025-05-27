package org.example.events

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class QuickSearch(
    date: Option[LocalDateTime],
    id: Option[Int],
    query: String,
    docIds: List[String],
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
)

object QuickSearch {

  def parse(
      lines: BufferedIterator[String]
  ): QuickSearch = {
    val line = lines.next().split("\\s+", 3).tail
    val nextLine = lines.next().split("\\s+")

    val datePart = line(0)

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
      if (line.tail(0).startsWith("{")) line.tail.mkString(" ")
      else line.mkString(" ")

    val id = Try(nextLine(0).toInt.abs).toOption

    val docIds = if (id.isEmpty) nextLine.toList else nextLine.tail.toList

    QuickSearch(date, id, query.slice(1, query.length - 1), docIds)
  }
}
