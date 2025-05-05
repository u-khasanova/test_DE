package org.example.events
import org.example.parser.{DateTimeParser, DateTimeParts}

case class DocOpen(
                       date: DateTimeParts,
                       id: String,
                       docId: String,
                     )

object DocOpen {

  def parse(line: String): Option[DocOpen] = {
    if (line == null || line.trim.isEmpty) return None

    val parts = line.split("\\s+")
    if (parts.length < 4) return None

    try {
      val date = DateTimeParser.parse(parts(1)).getOrElse(return None)
      val id = parts(2).replaceAll("[^0-9]", "")
      if (id.isEmpty) return None
      val docId = parts(3)

      Some(DocOpen(date, id, docId))
    } catch {
      case _: Exception => None
    }
  }

}