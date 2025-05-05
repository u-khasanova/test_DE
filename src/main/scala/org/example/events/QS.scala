package org.example.events
import org.example.parser.{DateTimeParser, DateTimeParts}

case class QS(
               date: Option[DateTimeParts],
               id: String,
               query: String,
               docIds: List[String]
             )

object QS {

  def parse(line: String): Option[QS] = {
    if (line == null) return None

    val trimmed = line.trim
    if (!trimmed.startsWith("QS ")) return None

    try {
      val openBrace = trimmed.indexOf('{')
      val closeBrace = trimmed.indexOf('}', openBrace + 1)

      if (openBrace < 0 || closeBrace < openBrace) return None

      val beforeBrace = trimmed.substring(0, openBrace).trim
      val dateParts = beforeBrace.split("\\s+")
      if (dateParts.length < 2) return None

      val date = DateTimeParser.parse(dateParts.last) match {
        case Some(dt) => Some(dt)
        case None => return None
      }

      val query = trimmed.substring(openBrace + 1, closeBrace).trim

      val afterBrace = trimmed.substring(closeBrace + 1).trim
      if (afterBrace.isEmpty) return None

      val afterParts = afterBrace.split("\\s+")
      val id = afterParts.head.replaceAll("[^0-9]", "")
      if (id.isEmpty) return None

      val docIds = if (afterParts.length > 1) afterParts.tail.toList else Nil

      Some(QS(date, id, query, docIds))
    } catch {
      case _: Exception => None
    }
  }

}