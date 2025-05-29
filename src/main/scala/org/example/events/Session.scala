package org.example.events

import org.example.errorProcessors.{ErrorAccumulator, ParseError}
import org.example.processors.RawDataProcessor.ParseContext
import org.example.processors.SessionBuilder

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.{Success, Try}

case class Session(
    file: String,
    startDate: Option[LocalDateTime],
    endDate: Option[LocalDateTime],
    quickSearches: List[QuickSearch],
    cardSearches: List[CardSearch],
    docOpens: List[DocOpen]
)

object Session {

  def parse(context: ParseContext): ParseContext = {
    try {
      while (context.iterator.hasNext) {

        val line = context.iterator.head

        val lineStart = context.iterator.head
          .split(" ")(0)

        line match {

          case _ if lineStart == "SESSION_START" =>
            if (context.currentSession.endDate.nonEmpty) {
              context.currentSession = SessionBuilder(context.filePath)
            }
            context.currentSession.startDate = parseDateTime(context.iterator)

          case _ if lineStart == "QS" =>
            QuickSearch.parse(context)

          case _ if lineStart == "CARD_SEARCH_START" =>
            CardSearch.parse(context)

          case _ if lineStart == "DOC_OPEN" =>
            DocOpen.parse(context)

          case _ if lineStart == "SESSION_END" =>
            context.currentSession.endDate = parseDateTime(context.iterator)
            context.sessions += context.currentSession
            checkTrailingLines(context)

          case _ => if (context.iterator.hasNext) context.iterator.next()
        }
      }
    } catch {
      case e: Exception =>
        val err = ParseError(
          context.filePath,
          context.iterator.head,
          s"${e.getStackTrace.head}",
          e.getClass.getSimpleName,
          e.getMessage
        )
        context.errorAccumulator.asInstanceOf[ErrorAccumulator].add(err)

    }
    context
  }

  private def parseDateTime(
      lines: BufferedIterator[String]
  ): Option[LocalDateTime] = {

    val line = lines.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    val dateFormats = Array(
      DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss"),
      DateTimeFormatter.ofPattern("EEE,_dd_MMM_yyyy_HH:mm:ss_XXXX", Locale.US)
    )

    dateFormats
      .map { formatter =>
        Try(LocalDateTime.parse(datePart, formatter))
      }
      .collectFirst { case Success(date) =>
        date
      }
  }

  private def checkTrailingLines(context: ParseContext): Unit = {

    while (
      context.iterator.hasNext && !context.iterator.head.startsWith(
        "SESSION_START"
      )
    ) {
      println(
        s"WARNING: Found non-session lines after SESSION_END in ${context.filePath}"
      )
      println(s"  Example: ${context.iterator.next()}")
    }
  }
}
