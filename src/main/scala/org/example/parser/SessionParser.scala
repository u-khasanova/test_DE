package org.example.parser

import org.apache.spark.util.LongAccumulator
import org.example.Main.{ParseError, logger}
import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.processors.SessionBuilder

object SessionParser {

  def parse(
      content: String,
      filePath: String,
      errorAccumulator: LongAccumulator
  ): Option[Session] = {

    val lines = content
      .split("\n")
      .iterator
      .buffered

    var currentSession: Option[SessionBuilder] = None

    while (lines.hasNext) {
      val line = lines.head
      try {
        line match {
          case l if l.startsWith("SESSION_START") =>
            currentSession =
              parseStart(l, lines, filePath, currentSession, errorAccumulator)

          case l if l.startsWith("SESSION_END") =>
            currentSession =
              parseEnd(l, lines, filePath, currentSession, errorAccumulator)

          case l if l.startsWith("QS") && currentSession.isDefined =>
            val sessionDate =
              if (
                currentSession.get.startDate.date == currentSession.get.endDate.date
              ) {
                Some(currentSession.get.startDate)
              } else None
            QuickSearch
              .parse(lines, filePath, errorAccumulator, sessionDate)
              .foreach(currentSession.get.quickSearches += _)

          case l
              if l.startsWith(
                "CARD_SEARCH_START"
              ) && currentSession.isDefined =>
            val sessionDate =
              if (
                currentSession.get.startDate.date == currentSession.get.endDate.date
              ) {
                Some(currentSession.get.startDate)
              } else None
            CardSearch
              .parse(lines, filePath, errorAccumulator, sessionDate)
              .foreach(currentSession.get.cardSearches += _)

          case l if l.startsWith("DOC_OPEN") && currentSession.isDefined =>
            val sessionDate =
              if (
                currentSession.get.startDate.date == currentSession.get.endDate.date
              ) {
                Some(currentSession.get.startDate)
              } else None
            DocOpen
              .parse(lines, filePath, errorAccumulator, sessionDate)
              .foreach(currentSession.get.docOpens += _)

          case _ => lines.next()
        }
      } catch {
        case e: Exception =>
          errorAccumulator.add(1)
          val err = ParseError(
            filePath,
            line,
            e.getClass.getSimpleName,
            e.getMessage,
            "Session"
          )
          logger.error(err.toLogString)
          if (lines.hasNext) lines.next()
      }
    }
    currentSession.flatMap(_.buildWithRecoveredIds())
  }

  private def parseStart(
      line: String,
      lines: BufferedIterator[String],
      filePath: String,
      currentSession: Option[SessionBuilder],
      errorAccumulator: LongAccumulator
  ): Option[SessionBuilder] = {
    currentSession.flatMap(_.build())
    lines.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    DateTime.parse(datePart) match {
      case dt =>
        Some(SessionBuilder(dt, dt))

      case e =>
        val err = ParseError(
          filePath,
          line,
          "InvalidDateTimeFormat",
          s"Invalid SESSION_START datetime: $e",
          "Session"
        )
        logger.error(err.toLogString)
        errorAccumulator.add(1)
        None
    }
  }

  private def parseEnd(
      line: String,
      lines: BufferedIterator[String],
      filePath: String,
      currentSession: Option[SessionBuilder],
      errorAccumulator: LongAccumulator
  ): Option[SessionBuilder] = {
    lines.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    val result = DateTime.parse(datePart) match {
      case dt =>
        currentSession.map { sb =>
          sb.endDate = dt
          sb
        }

      case e =>
        val err = ParseError(
          filePath,
          line,
          "InvalidDateTimeFormat",
          s"Invalid SESSION_END datetime: $e",
          "Session"
        )
        logger.error(err.toLogString)
        errorAccumulator.add(1)
        currentSession
    }
    if (lines.hasNext) lines.next()
    result
  }
}
