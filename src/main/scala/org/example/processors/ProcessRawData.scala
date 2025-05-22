package org.example.processors

import org.apache.spark.util.CollectionAccumulator
import org.example.Main.ParseError
import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}

object ProcessRawData {

  def process(
      content: String,
      filePath: String,
      errorAccumulator: CollectionAccumulator[String],
      recoverId: Boolean,
      recoverEmptyDate: Boolean
  ): Option[Session] = {

    val lines = content
      .split("\n")
      .iterator
      .buffered

    val currentSession = BuildSession(filePath)
    currentSession.build()

    while (lines.hasNext) {
      val line = lines.head
      try {
        line match {
          case l if l.split(" ")(0) == "SESSION_START" =>
            currentSession.startDate = Session.parseStart(lines)

          case l if l.split(" ")(0) == "QS" =>
            val qs = QuickSearch
              .parse(lines)
            currentSession.quickSearches += qs

          case l if l.split(" ")(0) == "CARD_SEARCH_START" =>
            val cs = CardSearch
              .parse(lines)
            currentSession.cardSearches += cs

          case l if l.split(" ")(0) == "DOC_OPEN" =>
            val docOpen = DocOpen
              .parse(lines)
            currentSession.docOpens += docOpen

          case l if l.split(" ")(0) == "SESSION_END" =>
            currentSession.endDate = Session.parseEnd(lines)

          case _ => if (lines.hasNext) lines.next()
        }
      } catch {
        case e: Exception =>
          val err = ParseError(
            filePath,
            line,
            e.getClass.getSimpleName,
            e.getMessage
          )
          errorAccumulator.add(err.toLogString)
      }
    }
    if (recoverId && recoverEmptyDate) {
      currentSession.buildWithRecoveredIdsAndRecoveredDates()
    } else if (!recoverId && recoverEmptyDate) {
      currentSession.buildWithRecoveredDates()
    } else if (recoverId && !recoverEmptyDate) {
      currentSession.buildWithRecoveredIds()
    } else currentSession.buildClean()
  }
}
