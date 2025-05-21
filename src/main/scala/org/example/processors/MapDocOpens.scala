package org.example.processors

import org.example.events.{DocOpen, Session}

import scala.collection.mutable

object MapDocOpens {

  def mapDocOpens(session: Session): Session = {
    val (quickSearchesWithOpens, remainingDocOpens) =
      mapToSearches(session.quickSearches, session.docOpens)

    val (cardSearchesWithOpens, finalDocOpens) =
      mapToSearches(session.cardSearches, remainingDocOpens)

    session.copy(
      quickSearches = quickSearchesWithOpens,
      cardSearches = cardSearchesWithOpens,
      docOpens = finalDocOpens
    )
  }

  private def mapToSearches[
      S <: {
        def id: Option[Int]
        def docOpens: mutable.ListBuffer[DocOpen]
      }
  ](
      searches: List[S],
      docOpens: List[DocOpen]
  ): (List[S], List[DocOpen]) = {
    val searchMap = searches
      .filter(_.id.isDefined)
      .groupBy(_.id.get)

    val (matched, unmatched) = docOpens.partition { docOpen =>
      docOpen.id.exists(searchMap.contains)
    }

    matched.foreach { docOpen =>
      searchMap(docOpen.id.get).foreach { search =>
        search.docOpens += docOpen
      }
    }

    (searches, unmatched)
  }
}
