package org.example.parser
import org.apache.spark.rdd.RDD
import org.example.events.{CardSearch, DocOpen, QS, Session}
import scala.collection.mutable

object SessionParser {
  case class SessionBuilder(
                                     startDate: DateTimeParts,
                                     var endDate: DateTimeParts,
                                     QSs: mutable.ListBuffer[QS] = mutable.ListBuffer.empty,
                                     cardSearches: mutable.ListBuffer[CardSearch] = mutable.ListBuffer.empty,
                                     docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
                                   ) {
    def addQS(qs: QS): Unit = QSs += qs
    def addCardSearch(cs: CardSearch): Unit = cardSearches += cs
    def addDocOpen(doc: DocOpen): Unit = docOpens += doc

    def build(): Option[Session] = {
      Some(Session(startDate, endDate, QSs.toList, cardSearches.toList, docOpens.toList))
    }
  }

  object SessionHolder {
    @transient private var currentSession: Option[SessionBuilder] = None
    @transient private var results: mutable.ListBuffer[Session] = mutable.ListBuffer.empty
    @transient private var pendingQSLine: Option[String] = None
    @transient private var inCardSearch: Boolean = false
    @transient private var pendingCardSearchLines: mutable.ListBuffer[String] = mutable.ListBuffer.empty
    @transient private var processPostEndLine: Boolean = false

    def initIfNeeded(): Unit = {
      if (currentSession == null) currentSession = None
      if (results == null) results = mutable.ListBuffer.empty
      if (pendingQSLine == null) pendingQSLine = None
      if (pendingCardSearchLines == null) pendingCardSearchLines = mutable.ListBuffer.empty
    }

    def getSession: Option[SessionBuilder] = currentSession
    def getResults: mutable.ListBuffer[Session] = results
    def getPendingQSLine: Option[String] = pendingQSLine
    def getInCardSearch: Boolean = inCardSearch
    def getPendingCardSearchLines: mutable.ListBuffer[String] = pendingCardSearchLines
    def getProcessPostEndLine: Boolean = processPostEndLine

    def setSession(session: Option[SessionBuilder]): Unit = currentSession = session
    def addResult(session: Session): Unit = results += session
    def setPendingQSLine(line: Option[String]): Unit = pendingQSLine = line
    def setInCardSearch(state: Boolean): Unit = inCardSearch = state
    def addPendingCardSearchLine(line: String): Unit = pendingCardSearchLines += line
    def setProcessPostEndLine(state: Boolean): Unit = processPostEndLine = state

    def clearSession(): Unit = currentSession = None
    def clearPendingQSLine(): Unit = pendingQSLine = None
    def clearCardSearchState(): Unit = {
      pendingCardSearchLines.clear()
      inCardSearch = false
    }
    def resetAll(): Unit = {
      currentSession = None
      results.clear()
      pendingQSLine = None
      inCardSearch = false
      pendingCardSearchLines.clear()
    }
  }

  def parse(sessions: RDD[String]): RDD[Session] = {
    sessions.mapPartitions { iter =>
      SessionHolder.initIfNeeded()

      def closeCurrentSession(): Unit = {
        SessionHolder.getSession.flatMap(_.build()).foreach { session =>
          SessionHolder.addResult(session)
        }
        SessionHolder.clearSession()
      }

      def handleSessionStart(line: String): Unit = {
        line.split(" ") match {
          case Array(_, timestamp) =>
            DateTimeParser.parse(timestamp) match {
              case Some(dt) =>
                SessionHolder.setSession(Some(SessionBuilder(startDate = dt, endDate = dt)))
            }
        }
      }

      def handleSessionEnd(line: String): Unit = {
        line.split(" ") match {
          case Array(_, timestamp) =>
            SessionHolder.getSession match {
              case Some(builder) =>
                DateTimeParser.parse(timestamp) match {
                  case Some(dt) =>
                    builder.endDate = dt
                    closeCurrentSession()
                  case None =>
                    System.err.println(s"Invalid SESSION_END timestamp: $timestamp")
                }
              case None =>
                System.err.println(s"SESSION_END without active session: $line")
            }
          case _ => System.err.println(s"Malformed SESSION_END: $line")
        }
      }

      def processQSBlock(firstLine: String, secondLine: String): Unit = {
        SessionHolder.getSession.foreach { builder =>
          QS.parse(s"$firstLine $secondLine").foreach(builder.addQS)
        }
        SessionHolder.clearPendingQSLine()
      }

      def processCardSearchBlock(): Unit = {
        SessionHolder.getSession.foreach { builder =>
          CardSearch.parse(
            SessionHolder.getPendingCardSearchLines.mkString(" ")
          ).foreach(builder.addCardSearch)
        }
        SessionHolder.clearCardSearchState()
      }

      def synchronizedProcess[T](block: => T): T = SessionHolder.synchronized {
        block
      }

      iter.foreach { line =>
        synchronizedProcess {
          line match {
            case l if l.startsWith("SESSION_START") =>
              handleSessionStart(l)

            case l if l.startsWith("SESSION_END") =>
              if (SessionHolder.getSession.isDefined) {
                handleSessionEnd(l)
              } else {
                System.err.println(s"SESSION_END without active session: $l")
              }

            case l if l.startsWith("CARD_SEARCH_START") =>
              SessionHolder.clearCardSearchState()
              SessionHolder.setInCardSearch(true)
              SessionHolder.addPendingCardSearchLine(l)

            case l if l.startsWith("CARD_SEARCH_END") =>
              if (SessionHolder.getInCardSearch) {
                SessionHolder.addPendingCardSearchLine(l)
                SessionHolder.setProcessPostEndLine(true)
              }

            case l if SessionHolder.getProcessPostEndLine =>
              SessionHolder.addPendingCardSearchLine(l)
              processCardSearchBlock()
              SessionHolder.setProcessPostEndLine(false)
              SessionHolder.setInCardSearch(false)

            case l if SessionHolder.getInCardSearch =>
              SessionHolder.addPendingCardSearchLine(l)

            case l if l.startsWith("QS") =>
              SessionHolder.clearPendingQSLine()
              SessionHolder.setPendingQSLine(Some(l))

            case l if SessionHolder.getPendingQSLine.isDefined =>
              processQSBlock(SessionHolder.getPendingQSLine.get, l)
              SessionHolder.clearPendingQSLine()

            case l if l.startsWith("DOC_OPEN") =>
              SessionHolder.getSession.foreach { builder =>
                DocOpen.parse(l).foreach(builder.addDocOpen)
              }

            case _ =>
              None
          }
        }
      }
      SessionHolder.getResults.iterator
    }
  }
}