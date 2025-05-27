package org.example.processors

import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}

import java.time.LocalDateTime

object EmptyDateProcessor {

  def recover(session: Session): Session = {
    val sessionDate = session.startDate

    session.copy(
      quickSearches = recoverQuickSearches(session.quickSearches, sessionDate),
      cardSearches = recoverCardSearches(session.cardSearches, sessionDate),
      docOpens = recoverDocOpens(session.docOpens, sessionDate)
    )
  }

  private def recoverQuickSearches(
      searches: List[QuickSearch],
      sessionDate: Option[LocalDateTime]
  ): List[QuickSearch] = {
    searches.map { qs =>
      if (qs.date.isEmpty) qs.copy(date = sessionDate) else qs
    }
  }

  private def recoverCardSearches(
      searches: List[CardSearch],
      sessionDate: Option[LocalDateTime]
  ): List[CardSearch] = {
    searches.map { cs =>
      if (cs.date.isEmpty) cs.copy(date = sessionDate) else cs
    }
  }

  private def recoverDocOpens(
      opens: List[DocOpen],
      sessionDate: Option[LocalDateTime]
  ): List[DocOpen] = {
    opens.map { doc =>
      if (doc.date.isEmpty) doc.copy(date = sessionDate) else doc
    }
  }
}
