package org.example.events

import org.example.errorProcessors.EmptyFieldLogger
import org.example.processors.DateTimeProcessor
import org.example.processors.RawDataProcessor.ParseContext

import java.time.LocalDateTime
import scala.collection.mutable
import scala.util.Try

case class CardSearch(
    var date: Option[LocalDateTime],
    var searchId: Option[Int],
    query: String,
    docIds: List[String],
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
)

object CardSearch {

  def parse(
      context: ParseContext
  ): Unit = {
    val content = new mutable.StringBuilder(context.iterator.next().trim)
    var foundEnd = false

    while (context.iterator.hasNext && !foundEnd) {
      val nextLine = context.iterator.next()
      content.append(s" $nextLine")
      foundEnd = nextLine.trim.startsWith("CARD_SEARCH_END")
      if (foundEnd) content.append(s" ${context.iterator.next()}")
    }

    val fullContent = content.toString()

    val parts = fullContent.split("CARD_SEARCH_END")
    val beforeEnd = parts(0).trim.split("\\s+").tail
    val afterEnd = parts(1).trim.split("\\s+")

    val datePart = beforeEnd(0)

    val date = DateTimeProcessor.process(datePart)

    val query =
      if (date.isEmpty) beforeEnd.mkString(" ")
      else beforeEnd.tail.mkString(" ")
    val searchId = Try(afterEnd.head.toInt.abs).toOption
    val docIds = if (searchId.isEmpty) afterEnd.toList else afterEnd.tail.toList

    Seq(
      ("date", date),
      ("searchId", searchId)
    ).foreach { case (name, value) =>
      EmptyFieldLogger.log(
        context,
        name,
        value,
        fullContent,
        "CardSearch.parse"
      )
    }

    context.currentSession.cardSearches += CardSearch(
      date,
      searchId,
      query,
      docIds
    )
  }
}
