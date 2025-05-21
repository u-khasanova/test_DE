package org.example.processors

import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.fields.DateTime

import scala.collection.mutable

case class SessionBuilder(
    file: String,
    var startDate: Option[DateTime] = None,
    var endDate: Option[DateTime] = None,
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
    )
  }

  def buildClean(): Option[Session] = {
    Some(
      Session(
        file,
        startDate,
        endDate,
        quickSearches.toList,
        cardSearches.toList,
        docOpens.toList
      )
    ).map(MapDocOpens.mapDocOpens)
  }

  def buildWithRecoveredIdsAndRecoveredDates(): Option[Session] = {
    build()
      .map(RecoverID.recover)
      .map(RecoverEmptyDate.recover)
      .map(MapDocOpens.mapDocOpens)
  }

  def buildWithRecoveredDates(): Option[Session] = {
    build()
      .map(RecoverEmptyDate.recover)
      .map(MapDocOpens.mapDocOpens)
  }

  def buildWithRecoveredIds(): Option[Session] = {
    build()
      .map(RecoverID.recover)
      .map(MapDocOpens.mapDocOpens)
  }
}
