package org.example.events

import org.example.errorProcessors.EmptyFieldLogger
import org.example.processors.DateTimeProcessor
import org.example.processors.RawDataProcessor.ParseContext

import java.time.LocalDateTime
import scala.util.Try

case class DocOpen(
    var date: Option[LocalDateTime],
    var searchId: Option[Int],
    docId: Option[String]
)

object DocOpen {
  def parse(
      context: ParseContext
  ): Unit = {
    val content = context.iterator.next()
    val parts = content.split("\\s+").tail
    val datePart = parts(0)

    val date = DateTimeProcessor.process(datePart)

    val searchId =
      if (date.isEmpty && parts(0).matches("^[0-9].*"))
        Try(parts(0).toInt.abs).toOption
      else Try(parts(1).toInt.abs).toOption

    val docId =
      if (parts.last.matches("^[0-9].*")) None else Try(parts.last).toOption

    Seq(("date", date), ("searchId", searchId), ("docId", docId))
      .foreach { case (name, value) =>
        EmptyFieldLogger.log(context, name, value, content, "DocOpen.parse")
      }

    context.currentSession.docOpens +=
      DocOpen(date, searchId, docId)
  }
}
