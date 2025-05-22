package org.example.events

import org.example.fields.DateTime
import scala.util.Try

case class DocOpen(
    date: Option[DateTime],
    id: Option[Int],
    docId: Option[String]
)

object DocOpen {
  def parse(
      lines: BufferedIterator[String]
  ): DocOpen = {
    val content = lines.next()
    val parts = content.split("\\s+").tail
    val date = DateTime.parse(parts(0))

    val id =
      if (date.isEmpty && parts(0).matches("^[0-9].*"))
        Try(parts(0).toInt.abs).toOption
      else Try(parts(1).toInt.abs).toOption

    val docId =
      if (parts.last.matches("^[0-9].*")) None else Try(parts.last).toOption

    DocOpen(date, id, docId)
  }
}
