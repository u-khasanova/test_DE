package org.example.events

import org.example.fields.DateTime
import scala.collection.mutable
import scala.util.Try

case class CardSearch(
    date: Option[DateTime],
    id: Option[Int],
    query: String,
    docIds: List[String],
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
)

object CardSearch {

  def parse(
      lines: BufferedIterator[String]
  ): CardSearch = {
    extract(lines)
  }

  def extract(
      lines: BufferedIterator[String]
  ): CardSearch = {
    val content = new mutable.StringBuilder(lines.next().trim)
    var foundEnd = false

    while (lines.hasNext && !foundEnd) {
      val nextLine = lines.next()
      content.append(s" $nextLine")
      foundEnd = nextLine.trim.startsWith("CARD_SEARCH_END")
      if (foundEnd) content.append(s" ${lines.next()}")
    }

    val fullContent = content.toString()

    val parts = fullContent.split("CARD_SEARCH_END")
    val beforeEnd = parts(0).trim.split("\\s+", 3)
    val afterEnd = parts(1).trim.split("\\s+")

    val date = DateTime.parse(beforeEnd(1))
    val query = beforeEnd(2)
    val id = Try(afterEnd.head.toInt.abs).toOption
    val docIds = if (id.isEmpty) afterEnd.toList else afterEnd.tail.toList

    CardSearch(date, id, query, docIds)
  }
}
