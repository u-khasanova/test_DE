package org.example.events

import org.example.fields.DateTime
import scala.collection.mutable
import scala.util.Try

case class QuickSearch(
    date: Option[DateTime],
    id: Option[Int],
    query: String,
    docIds: List[String],
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
)

object QuickSearch {

  def parse(
      lines: BufferedIterator[String]
  ): QuickSearch = {
    val line = lines.next().trim
    val nextLine = lines.next().trim
    val content = s"$line $nextLine"

    extract(content)
  }

  def extract(
      content: String
  ): QuickSearch = {
    val openBrace = content.indexOf('{')
    val closeBrace = content.indexOf('}', openBrace + 1)

    val date = DateTime.parse(
      content
        .substring(0, openBrace)
        .split("\\s+")
        .last
    )

    val query = content
      .substring(openBrace + 1, closeBrace)
      .trim

    val parts = content
      .substring(closeBrace + 1)
      .trim
      .split("\\s+")

    val id = Try(parts(0).toInt.abs).toOption

    val docIds = if (id.isEmpty) parts.toList else parts.tail.toList

    QuickSearch(date, id, query, docIds)
  }
}
