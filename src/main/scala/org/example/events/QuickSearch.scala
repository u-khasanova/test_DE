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
    val line = lines.next().split("\\s+", 3).tail
    val nextLine = lines.next().split("\\s+")

    val date = DateTime.parse(line(0))

    val query =
      if (line.tail(0).startsWith("{")) line.tail.mkString(" ")
      else line.mkString(" ")

    val id = Try(nextLine(0).toInt.abs).toOption

    val docIds = if (id.isEmpty) nextLine.toList else nextLine.tail.toList

    QuickSearch(date, id, query.slice(1, query.length - 1), docIds)
  }
}
