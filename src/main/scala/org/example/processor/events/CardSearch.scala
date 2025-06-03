package org.example.processor.events

import org.example.processor.DateTimeParser
import org.example.processor.utils.ParseContext

import java.time.LocalDateTime
import scala.collection.mutable
import scala.util.Try

case class CardSearch(
    var date: Option[LocalDateTime],
    var searchId: Option[Int],
    query: List[(String, String)],
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

    val date = DateTimeParser.process(context, beforeEnd(0))

    val query = {
      val str = if (date.isEmpty) beforeEnd.mkString(" ") else beforeEnd.tail.mkString(" ")

      str
        .split("\\s*\\$\\s*")
        .filter(_.nonEmpty)
        .map { part =>
          val arr = part.split("\\s+", 2)
          (arr(0), if (arr.length > 1) arr(1) else "")
        }
        .toList
    }

    val searchId = Try(afterEnd.head.toInt.abs).toOption

    if (searchId.isEmpty) context.addEmptyFieldWarning("CardSearch.parse", "searchId")

    val docIds = if (searchId.isEmpty) afterEnd.toList else afterEnd.tail.toList

    context.currentSession.cardSearches += CardSearch(
      date,
      searchId,
      query,
      docIds
    )
  }
}
