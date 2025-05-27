package org.example.events

import org.example.processors.RawDataProcessor.ParseContext
import org.example.processors.SessionBuilder

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.{Failure, Success, Try}

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
    while (context.iterator.hasNext) {

      val line = context.iterator.head

      val lineStart = context.iterator.head
        .split(" ")(0)

      line match {

        case _ if lineStart == "SESSION_START" =>
          if (context.currentSession.endDate.nonEmpty) {
            context.currentSession = SessionBuilder(context.filePath)
          }
          context.currentSession.startDate = parseStart(context.iterator)

        case _ if lineStart == "QS" =>
          val qs = QuickSearch
            .parse(context.iterator)
          context.currentSession.quickSearches += qs

        case _ if lineStart == "CARD_SEARCH_START" =>
          val cs = CardSearch
            .parse(context.iterator)
          context.currentSession.cardSearches += cs

        case _ if lineStart == "DOC_OPEN" =>
          val docOpen = DocOpen
            .parse(context.iterator)
          context.currentSession.docOpens += docOpen

        case _ if lineStart == "SESSION_END" =>
          context.currentSession.endDate = parseEnd(context.iterator)
          context.sessions += context.currentSession
          checkTrailingLines(context)

        case _ => if (context.iterator.hasNext) context.iterator.next()
      }
    }
    context
  }

  private def parseStart(
      lines: BufferedIterator[String]
  ): Option[LocalDateTime] = {

    val line = lines.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    Try(
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
  }

  private def parseEnd(
      lines: BufferedIterator[String]
  ): Option[LocalDateTime] = {

    val line = lines.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    Try(
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
