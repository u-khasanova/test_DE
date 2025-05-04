package org.example.parser
import org.example.events.QS

object QSParser {
  def parse(line: String): Option[QS] = {
    val trimmed = line.trim
    if (!trimmed.startsWith("QS ")) return None
    val content = trimmed.drop(0)
    val openBrace = content.indexOf('{')
    val closeBrace = content.indexOf('}', openBrace + 1)
    if (openBrace < 0 || closeBrace < openBrace) return None
    val beforeBrace = content.take(openBrace).trim
    val afterBrace = content.drop(closeBrace + 1).trim
    val dateParts = beforeBrace.split("\\s+")
    if (dateParts.isEmpty) return None
    val date = DateTimeParser.parse(dateParts.last)
    val afterParts = afterBrace.split("\\s+")
    if (afterParts.length < 1) return None
    val id = afterParts.head.replaceAll("[^0-9]", "")
    if (id.isEmpty) return None
    val docIds = if (afterParts.length > 1) {
      afterParts.tail.mkString(" ").split("\\s+").toList
    } else {
      Nil
    }
    val query = content.substring(openBrace + 1, closeBrace).trim
    Some(QS(date, query, id, docIds))
  }
}