package org.example.processors

import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.parser.DateTime

import scala.collection.mutable

case class SessionBuilder(
    startDate: DateTime,
    var endDate: DateTime,
    quickSearches: mutable.ListBuffer[QuickSearch] = mutable.ListBuffer.empty,
    cardSearches: mutable.ListBuffer[CardSearch] = mutable.ListBuffer.empty,
    docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty
) {
  def build(): Option[Session] = {
    Some(
      Session(
        startDate,
        endDate,
        quickSearches.toList,
        cardSearches.toList,
        docOpens.toList
      )
    )
  }

  def buildWithRecoveredIds(): Option[Session] = {
    build().map(RecoverID.recoverIds)
  }
}
