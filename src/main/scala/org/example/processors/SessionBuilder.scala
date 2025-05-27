package org.example.processors

import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}
import org.example.processors.RawDataProcessor.ParseContext

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
  def build(context: ParseContext): Option[Session] = {
    if (context.recoverId && context.recoverEmptyDate) {
      buildWithRecoveredIdsAndRecoveredDates()
    } else if (!context.recoverId && context.recoverEmptyDate) {
      buildWithRecoveredDates()
    } else if (context.recoverId && !context.recoverEmptyDate) {
      buildWithRecoveredIds()
    } else {
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

  private def buildWithRecoveredIdsAndRecoveredDates(): Option[Session] = {
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
      .map(EmptyIdProcessor.recover)
      .map(EmptyDateProcessor.recover)
      .map(DocOpenMapper.mapDocOpens)
  }

  private def buildWithRecoveredDates(): Option[Session] = {
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
      .map(EmptyDateProcessor.recover)
      .map(DocOpenMapper.mapDocOpens)
  }

  private def buildWithRecoveredIds(): Option[Session] = {
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
      .map(EmptyIdProcessor.recover)
      .map(DocOpenMapper.mapDocOpens)
  }
}
