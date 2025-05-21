package org.example.fields

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.{Failure, Success, Try}

case class DateTime(date: String, time: String) {
  override def toString: String = s"$date $time"
}

class DateParseException(message: String, input: String)
    extends IllegalArgumentException(s"$message. Input: '$input'")

object DateTime {

  private val primaryFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  private val secondaryFormat =
    DateTimeFormatter.ofPattern("dd_MMM_yyyy_HH:mm:ss", Locale.US)

  def parse(dateStr: String): Option[DateTime] = {
    Try(LocalDateTime.parse(dateStr, primaryFormat)) match {
      case Success(date) =>
        Some(
          DateTime(
            date.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")),
            date.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
          )
        )
      case Failure(e) =>
        Try(
          LocalDateTime.parse(
            dateStr.split("_").slice(1, 5).mkString("_"),
            secondaryFormat
          )
        ) match {
          case Success(date) =>
            Some(
              DateTime(
                date.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")),
                date.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
              )
            )
          case Failure(e) => None
        }
    }
  }
}
