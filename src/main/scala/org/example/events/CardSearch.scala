package org.example.events
import org.example.parser.{DateTimeParser, DateTimeParts}

case class CardSearch(
               date: DateTimeParts,
               id: String,
               query: String,
               docIds: List[String]
             )