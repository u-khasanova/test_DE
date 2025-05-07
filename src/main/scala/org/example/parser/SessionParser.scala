package org.example.parser

import org.apache.spark.rdd.RDD
import org.example.events.{CardSearch, DocOpen, QS, Session}
import scala.collection.mutable

object SessionParser {

  private case class SessionBuilder(
                                     startDate: DateTimeParts,
                                     var endDate: DateTimeParts,
                                     QSs: mutable.ListBuffer[QS] = mutable.ListBuffer.empty,
                                     cardSearches: mutable.ListBuffer[CardSearch] = mutable.ListBuffer.empty,
                                     docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
                                   ) {
    def build(): Option[Session] = {
      Some(Session(startDate, endDate, QSs.toList, cardSearches.toList, docOpens.toList))
    }
  }

  def parse(content: String): Option[Session] = {
    val lines = content.split("\n").iterator
    var currentSession: Option[SessionBuilder] = None

    def processLine(line: String): Unit = {
      line.trim match {
        case l if l.startsWith("SESSION_START") =>
          currentSession.flatMap(_.build())

          l.split(" ").drop(1).headOption.flatMap(DateTimeParser.parse) match {
            case Some(dt) => currentSession = Some(SessionBuilder(dt, dt))
            case None => System.err.println(s"Invalid SESSION_START timestamp: $l")
          }

        case l if l.startsWith("SESSION_END") && currentSession.isDefined =>
          l.split(" ").drop(1).headOption.flatMap(DateTimeParser.parse) match {
            case Some(dt) => currentSession.foreach(_.endDate = dt)
            case None => System.err.println(s"Invalid SESSION_END timestamp: $l")
          }

        case l if l.startsWith("QS") && currentSession.isDefined =>
          val qsContent = new mutable.StringBuilder(l)
          if (lines.hasNext) {
            val nextLine = lines.next().trim
            qsContent.append(" ").append(nextLine)
          }
          QS.parse(qsContent.toString).foreach { qs =>
            currentSession.foreach(_.QSs += qs)
          }

        case l if l.startsWith("CARD_SEARCH_START") && currentSession.isDefined =>
          val cardSearchLines = mutable.ListBuffer(l)

          var foundEnd = false
          while (lines.hasNext && !foundEnd) {
            val nextLine = lines.next()
            cardSearchLines += nextLine
            foundEnd = nextLine.trim.startsWith("CARD_SEARCH_END")
            if (foundEnd) {
              val nextLine = lines.next()
              cardSearchLines += nextLine
            }
          }

          if (!foundEnd) {
            System.err.println("Unclosed CARD_SEARCH block")
          } else {
            CardSearch.parse(cardSearchLines.mkString(" ")).foreach { cs =>
              currentSession.foreach(_.cardSearches += cs)
            }
          }

        case l if l.startsWith("DOC_OPEN") && currentSession.isDefined =>
          DocOpen.parse(l).foreach { doc =>
            currentSession.foreach(_.docOpens += doc)
          }

        case _ =>
      }
    }

    while (lines.hasNext) {
      processLine(lines.next())
    }

    currentSession.flatMap(_.build())
  }
}