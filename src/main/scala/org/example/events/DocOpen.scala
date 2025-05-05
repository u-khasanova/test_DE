package org.example.events
import org.example.parser.{DateTimeParser, DateTimeParts}

case class DocOpen(
                       date: DateTimeParts,
                       id: String,
                       docId: String,
                     )

object DocOpen {

  def parse(line: String): Option[DocOpen] = {
    val trimmed = line.split(" ")
    val date = DateTimeParser.parse(trimmed(1)).getOrElse(return None)
    val id = trimmed(2).replaceAll("[^0-9]", "")
    if (id.isEmpty) return None
    val docId = trimmed(3)
    if (docId.isEmpty) return None

    Some(DocOpen(date, id, docId))
  }

}