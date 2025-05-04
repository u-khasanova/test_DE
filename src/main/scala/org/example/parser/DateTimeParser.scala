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
    DateTimeFormatter.ofPattern("dd_MMM_yyyy'_'HH:mm:ss", Locale.US),  // 01_Aug_2020_13:48:07
    DateTimeFormatter.ofPattern("MMM_dd_yyyy_HH:mm:ss", Locale.US),  // Aug_01_2020_13:48:07
    DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss"),              // 01.07.2020_13:42:01
    DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"),              // 01-07-2020 13:42:01
    DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss"),              // 07-01-2020 13:42:01
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),              // 2020-07-01 13:48:07
    DateTimeFormatter.ofPattern("yyyy-dd-MM HH:mm:ss"),              // 2020-01-07 00:00:00
    DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a", Locale.US)  // 07/01/2020 01:48:07 PM
  )

  def parse(dateStr: String): Option[DateTimeParts] = {
    val cleanedStr = dateStr.replaceAll("^(Mon|Tue|Wed|Thu|Fri|Sat|Sun),?_?", "")

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

object DateTimeParserTest extends App {
  val testData = Seq(
    "01_Aug_2020_13:48:07",
    "Aug_01_2020_13:48:07",
    "Wed,_01_Aug_2020_13:48:07_+0300",
    "01.07.2020_13:42:01",
    "01-07-2020 13:42:01",
    "07-01-2020 13:42:01",
    "2020-07-01 13:48:07",
    "2020-01-07 00:00:00",
    "07/01/2020 01:48:07 PM",
    " " // add empty line parse option - logging to stderr???
  )

  testData.foreach { line =>
    println(s"\nСтрока: ${line.take(60)}...")
    DateTimeParser.parse(line) match {
      case Some(parts) =>
        println(s"Date: ${parts.date}")
        println(s"Time: ${parts.time}")
      case None =>
        println("Parse error")
    }
  }
}

