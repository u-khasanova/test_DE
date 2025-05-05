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
    val trimmed = line.split("CARD_SEARCH_END")
    val beforeEnd = trimmed(0).split(" ")
    val afterEnd = trimmed(1).split(" ")
    val date = DateTimeParser.parse(beforeEnd(1)).getOrElse(return None)
    val query = beforeEnd.drop(2).mkString(" ")
    val id = afterEnd.head.replaceAll("[^0-9]", "")
    if (id.isEmpty) return None

    val docIds = if (afterEnd.length > 1) {
      afterEnd.tail.mkString(" ").split("\\s+").toList
    } else {
      Nil
    }

    Some(CardSearch(date, id, query, docIds))
  }

}