package org.example
package parser
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

case class DateTimeParts(date: String, time: String)

object DateTimeParser extends Serializable {
  // can be extended
  private val knownDateFormats: List[DateTimeFormatter] = List(
    DateTimeFormatter.ofPattern(
      "dd_MMM_yyyy'_'HH:mm:ss",
      Locale.US
    ), // 01_Aug_2020_13:48:07
    DateTimeFormatter.ofPattern(
      "dd-MMM-yyyy HH:mm:ss",
      Locale.US
    ), // 01-Aug-2020 13:48:07
    DateTimeFormatter.ofPattern(
      "dd.MMM.yyyy'_'HH:mm:ss",
      Locale.US
    ), // 01.Aug.2020_13:48:07
    DateTimeFormatter.ofPattern(
      "dd/MMM/yyyy HH:mm:ss",
      Locale.US
    ), // 01/Aug/2020 13:48:07
    DateTimeFormatter.ofPattern(
      "MMM_dd_yyyy_HH:mm:ss",
      Locale.US
    ), // Aug_01_2020_13:48:07
    DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss") // 01.07.2020_13:42:01
  )

  def parse(dateStr: String): Option[DateTimeParts] = {
    if (dateStr == null || dateStr.trim.isEmpty) return None

    val cleanedStr = dateStr
      .replaceAll("^(Mon|Tue|Wed|Thu|Fri|Sat|Sun),?_", "")
      .replaceAll("_[+-]\\d{4}$", "")

    knownDateFormats.view.flatMap { formatter =>
      Try {
        val dateTime = LocalDateTime.parse(cleanedStr, formatter)
        formatParts(dateTime)
      }.toOption
    }.headOption
  }

  private def formatParts(dateTime: LocalDateTime): DateTimeParts = {
    DateTimeParts(
      date = dateTime.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")),
      time = dateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    )
  }
}
