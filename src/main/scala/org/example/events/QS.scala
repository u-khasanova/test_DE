package org.example.events
import org.example.parser.{DateTimeParser, DateTimeParts}

case class QS(
               date: Option[DateTimeParts],
               id: String,
               query: String,
               docIds: List[String]
             )