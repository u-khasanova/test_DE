package org.example.processors

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.{Success, Try}

object DateTimeProcessor {

  def process(date: String): Option[LocalDateTime] = {
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
  }
}
