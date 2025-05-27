package org.example.events

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.{Failure, Success, Try}

case class DocOpen(
    date: Option[LocalDateTime],
    id: Option[Int],
    docId: Option[String]
)

object DocOpen {
  def parse(
      lines: BufferedIterator[String]
  ): DocOpen = {
    val content = lines.next()
    val parts = content.split("\\s+").tail
    val datePart = parts(0)

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

    val id =
      if (date.isEmpty && parts(0).matches("^[0-9].*"))
        Try(parts(0).toInt.abs).toOption
      else Try(parts(1).toInt.abs).toOption

    val docId =
      if (parts.last.matches("^[0-9].*")) None else Try(parts.last).toOption

    DocOpen(date, id, docId)
  }
}
