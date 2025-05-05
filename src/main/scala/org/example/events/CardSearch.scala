package org.example.events
import org.example.parser.{DateTimeParser, DateTimeParts}


case class CardSearch(
               date: DateTimeParts,
               id: String,
               query: String,
               docIds: List[String]
             )

object CardSearch {

  def parse(line: String): Option[CardSearch] = {
    if (line == null || line.trim.isEmpty) return None

    val parts = line.split("CARD_SEARCH_END")
    if (parts.length < 2) return None

    try {
      val beforeEnd = parts(0).trim.split("\\s+", 3)
      if (beforeEnd.length < 2) return None

      val date = DateTimeParser.parse(beforeEnd(1)).getOrElse(return None)
      val query = if (beforeEnd.length > 2) beforeEnd(2) else ""

      val afterEnd = parts(1).trim.split("\\s+")
      if (afterEnd.isEmpty) return None

      val id = afterEnd.head.replaceAll("[^0-9]", "")
      if (id.isEmpty) return None

      val docIds = afterEnd.tail.toList

      Some(CardSearch(date, id, query, docIds))
    } catch {
      case _: Exception => None
    }
  }

}