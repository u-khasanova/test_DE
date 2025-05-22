package org.example.events

import org.example.fields.DateTime

case class Session(
    file: String,
    startDate: Option[DateTime],
    endDate: Option[DateTime],
    quickSearches: List[QuickSearch],
    cardSearches: List[CardSearch],
    docOpens: List[DocOpen]
)

object Session {

  def parseStart(
      lines: BufferedIterator[String]
  ): Option[DateTime] = {

    val line = lines.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    DateTime.parse(datePart) match {
      case dt => dt
    }
  }

  def parseEnd(
      lines: BufferedIterator[String]
  ): Option[DateTime] = {

    val line = lines.next()

    val datePart = line
      .split(" ")
      .lift(1)
      .getOrElse("")

    DateTime.parse(datePart) match {
      case dt => dt
    }
  }
}
