package org.example.processor

import org.example.processor.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.processor.fixers.DocOpenMapper

import java.time.LocalDateTime
import scala.collection.mutable

case class SessionBuilder(
    file: String,
    var startDate: Option[LocalDateTime] = None,
    var endDate: Option[LocalDateTime] = None,
    quickSearches: mutable.ListBuffer[QuickSearch] = mutable.ListBuffer.empty,
    cardSearches: mutable.ListBuffer[CardSearch] = mutable.ListBuffer.empty,
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
) {
  def build(): Option[Session] = {
    Some(
      Session(
        file,
        startDate,
        endDate,
        quickSearches.toList,
        cardSearches.toList,
        docOpens.toList
      )
    ).map(DocOpenMapper.mapDocOpens)
  }
}
