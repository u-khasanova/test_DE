package org.example.parser

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale

case class DateTime(date: String, time: String) {
  override def toString: String = s"$date $time"
}

class DateParseException(message: String, input: String)
    extends IllegalArgumentException(s"$message. Input: '$input'")

object DateTime {

  private val primaryFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  private val secondaryFormat =
    DateTimeFormatter.ofPattern("dd_MMM_yyyy_HH:mm:ss", Locale.US)

  def parse(dateStr: String): DateTime = {
    if (dateStr == null || dateStr.trim.isEmpty) {
      throw new DateParseException(
        "Date string cannot be null or empty",
        dateStr
      )
    }

    try {
      tryParse(dateStr, primaryFormat)
    } catch {
      case _: DateTimeParseException =>
        try {
          val newDate = dateStr.split("_").slice(1, 5).mkString("_")
          tryParse(newDate, secondaryFormat)
        } catch {
          case _: DateTimeParseException =>
            throw new DateParseException(
              s"Unrecognized date format. Supported formats: " +
                s"1) 'dd.MM.yyyy_HH:mm:ss' " +
                s"2) 'EEE,_dd_MMM_yyyy_HH:mm:ss_xx'",
              dateStr
            )
        }
    }
  }

  private def tryParse(
      dateStr: String,
      formatter: DateTimeFormatter
  ): DateTime = {
    val dateTime = LocalDateTime.parse(dateStr, formatter)
    DateTime(
      dateTime.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")),
      dateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    )
  }
}
