package org.example.processor.events

import org.example.processor.DateTimeParser
import org.example.processor.utils.ParseContext

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

  def parse(context: ParseContext): Session = {
    while (context.iterator.hasNext) {

      val lineStart = context.iterator.head
        .split(" ")(0)

      try {
        lineStart match {

          case "SESSION_START" =>
            parseDateTime(context, "start")

          case "QS" =>
            QuickSearch.parse(context)

          case "CARD_SEARCH_START" =>
            CardSearch.parse(context)

          case "DOC_OPEN" =>
            DocOpen.parse(context)

          case "SESSION_END" =>
            parseDateTime(context, "end")
            if (context.iterator.hasNext) {
              context.addTrailingLineAfterSessionEndWarning()
            }

          case _ => if (context.iterator.hasNext) context.iterator.next()
        }
      } catch {
        case e: Exception =>
          context.addError(e)
      }
    }
    context.currentSession.build()
  }

  private def parseDateTime(
      context: ParseContext,
      typeOfDate: String
  ): Unit = {

    val line = context.iterator.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    val date = DateTimeParser.process(context, datePart)

    if (typeOfDate == "start") {
      context.currentSession.startDate = date
    } else {
      context.currentSession.endDate = date
    }
  }
}
