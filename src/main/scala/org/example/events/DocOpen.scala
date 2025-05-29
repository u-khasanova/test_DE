package org.example.events

import org.example.processors.RawDataProcessor.ParseContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.{Success, Try}

case class DocOpen(
    date: Option[LocalDateTime],
    id: Option[Int],
    docId: Option[String]
)

object DocOpen {
  def parse(
      context: ParseContext
  ): Unit = {
    val content = context.iterator.next()
    val parts = content.split("\\s+").tail
    val datePart = parts(0)

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

    val id =
      if (date.isEmpty && parts(0).matches("^[0-9].*"))
        Try(parts(0).toInt.abs).toOption
      else Try(parts(1).toInt.abs).toOption

    val docId =
      if (parts.last.matches("^[0-9].*")) None else Try(parts.last).toOption

    context.currentSession.docOpens +=
      DocOpen(date, id, docId)
  }
}
