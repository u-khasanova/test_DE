package org.example.processor.fixers

import org.example.processor.events.Session

object EmptyDateFixer {

  def recover(session: Session): Session = {
    session.quickSearches.foreach { qs =>
      if (qs.date.isEmpty) qs.date = session.startDate
    }

    session.cardSearches.foreach { cs =>
      if (cs.date.isEmpty) cs.date = session.startDate
    }

    session.docOpens.foreach { docOpen =>
      if (docOpen.date.isEmpty) docOpen.date = session.startDate
    }

    session
  }
}
