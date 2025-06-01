package org.example.processor.fixers

import org.example.processor.events.{DocOpen, Session}

import scala.collection.mutable
import scala.language.reflectiveCalls

object DocOpenMapper {

  def mapDocOpens(session: Session): Session = {

    linkDocOpensToSearches(session.quickSearches, session.docOpens)
    linkDocOpensToSearches(session.cardSearches, session.docOpens)

    session
  }

  private def linkDocOpensToSearches[
      S <: {
        def searchId: Option[Int]
        def docOpens: mutable.ListBuffer[DocOpen]
      }
  ](searches: List[S], docOpens: List[DocOpen]): Unit = {
    val searchMap = searches
      .filter(_.searchId.isDefined)
      .groupBy(_.searchId.get)
      .view
      .toMap

    docOpens.foreach { docOpen =>
      docOpen.searchId.foreach { searchId =>
        searchMap
          .get(searchId)
          .foreach(_.foreach { search =>
            if (!search.docOpens.contains(docOpen)) {
              search.docOpens += docOpen
            }
          })
      }
    }
  }
}
