package org.example.processor.events

import org.example.processor.DateTimeParser
import org.example.processor.utils.ParseContext

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

    val date = DateTimeParser.process(context, parts(0))

    val searchId =
      if (date.isEmpty && parts(0).matches("^[0-9].*"))
        Try(parts(0).toInt.abs).toOption
      else Try(parts(1).toInt.abs).toOption

    if (searchId.isEmpty) context.addWarning("DocOpen.parse", "searchId")

    val docId = if (parts.last.matches("^[0-9].*")) None else Try(parts.last).toOption

    if (docId.isEmpty) context.addWarning("DocOpen.parse", "docId")

    context.currentSession.docOpens +=
      DocOpen(date, searchId, docId)
  }
}
