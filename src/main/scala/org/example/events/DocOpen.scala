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
    extract(content)
  }

  def extract(
      content: String
  ): DocOpen = {
    val parts = content.split("\\s+")
    val date = DateTime.parse(parts(1))

    val id = Try(parts(2).toInt.abs).toOption
      .orElse {
        Try(parts(1).toInt.abs).toOption
      }

    val docId = Try(parts(3)).toOption
      .filter(_.nonEmpty)
      .orElse(Try(parts(2)).toOption.filter(_.nonEmpty))

    DocOpen(date, id, docId)
  }
}
