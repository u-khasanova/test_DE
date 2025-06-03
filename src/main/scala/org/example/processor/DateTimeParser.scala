package org.example.processor

import org.example.processor.utils.ParseContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.{Success, Try}

object DateTimeParser {

  def process(context: ParseContext, date: String): Option[LocalDateTime] = {
    val dateFormats = Array(
      DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss"),
      DateTimeFormatter.ofPattern("EEE,_dd_MMM_yyyy_HH:mm:ss_XXXX", Locale.US)
    )

    dateFormats
      .map { formatter =>
        Try(LocalDateTime.parse(date, formatter))
      }
      .collectFirst { case Success(date) =>
        date
      }
      .orElse {
        context.addEmptyFieldWarning("DateTimeParser.process", "date")
        None
      }
  }
}
