package org.example.processor.events

import org.example.processor.utils.ParseContext
import org.example.processor.{DateTimeParser, SessionBuilder}

import java.time.LocalDateTime

case class Session(
    file: String,
    startDate: Option[LocalDateTime],
    endDate: Option[LocalDateTime],
    var quickSearches: List[QuickSearch],
    var cardSearches: List[CardSearch],
    var docOpens: List[DocOpen]
)

object Session {

  def parse(context: ParseContext): ParseContext = {
    try {
      while (context.iterator.hasNext) {

        val lineStart = context.iterator.head
          .split(" ")(0)

        lineStart match {

          case _ if lineStart == "SESSION_START" =>
            if (context.currentSession.endDate.nonEmpty) {
              context.currentSession = SessionBuilder(context.filePath)
            }
            context.currentSession.startDate = parseDateTime(context)

          case _ if lineStart == "QS" =>
            QuickSearch.parse(context)

          case _ if lineStart == "CARD_SEARCH_START" =>
            CardSearch.parse(context)

          case _ if lineStart == "DOC_OPEN" =>
            DocOpen.parse(context)

          case _ if lineStart == "SESSION_END" =>
            context.currentSession.endDate = parseDateTime(context)
            checkTrailingLines(context)

          case _ => if (context.iterator.hasNext) context.iterator.next()
        }
      }
    } catch {
      case e: Exception =>
        context.addError(e)
    }
    context
  }

  private def parseDateTime(
      context: ParseContext
  ): Option[LocalDateTime] = {

    val line = context.iterator.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    DateTimeParser.process(context, datePart)
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
