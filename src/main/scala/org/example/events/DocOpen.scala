package org.example.events
import org.example.parser.{DateTimeParser, DateTimeParts}

case class DocOpen(
                       date: DateTimeParts,
                       id: String,
                       docId: String,
                     )