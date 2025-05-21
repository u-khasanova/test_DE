package org.example.processors

import org.example.events.{CardSearch, QuickSearch, Session}

import scala.collection.mutable

object RecoverID {
  def recover(session: Session): Session = {

    /*
    Step 1: recover DocOpen ids based on quick / card search ids

    DocOpen must inherit id from parent search if DocOpen.docId:
     - belongs only to QuickSearch.docIds
    OR
     - belongs only to CardSearch.docIds
     */

    val docIdToSearchId = mutable.Map[String, Option[Int]]()
    val ambiguousDocs = mutable.Set[String]()

    (session.quickSearches ++ session.cardSearches).foreach {
      case qs: QuickSearch if qs.id.nonEmpty =>
        qs.docIds.foreach { docId =>
          if (docIdToSearchId.contains(docId)) ambiguousDocs.add(docId)
          else docIdToSearchId.put(docId, qs.id)
        }
      case cs: CardSearch if cs.id.nonEmpty =>
        cs.docIds.foreach { docId =>
          if (docIdToSearchId.contains(docId)) ambiguousDocs.add(docId)
          else docIdToSearchId.put(docId, cs.id)
        }
      case _ =>
    }

    val recoveredDocOpens = session.docOpens.map { docOpen =>
      if (docOpen.id.isEmpty) {
        docIdToSearchId.get(docOpen.docId.get) match {
          case Some(id) if !ambiguousDocs(docOpen.docId.get) =>
            docOpen.copy(id = id)
          case _ => docOpen
        }
      } else docOpen
    }

    /*
    Step 2: recover quick / card search

    Update search's id if at least one document was opened only within given search
     */

    val searchIdToDocIds = mutable.Map[Int, Set[String]]()

    recoveredDocOpens.foreach { docOpen =>
      if (docOpen.id.nonEmpty) {
        val currentDocs = searchIdToDocIds.getOrElse(docOpen.id.get, Set.empty)
        searchIdToDocIds.put(docOpen.id.get, currentDocs + docOpen.docId.get)
      }
    }

    val recoveredQuickSearches = session.quickSearches.map {
      case qs: QuickSearch if qs.id.isEmpty =>
        val possibleId = qs.docIds.flatMap { docId =>
          recoveredDocOpens
            .find(_.docId.contains(docId))
            .filter(_.id.nonEmpty)
            .map(_.id)
        }.headOption

        possibleId match {
          case Some(id) => qs.copy(id = id)
          case None     => qs
        }
      case qs => qs
    }

    val recoveredCardSearches = session.cardSearches.map {
      case cs: CardSearch if cs.id.isEmpty =>
        val possibleId = cs.docIds.flatMap { docId =>
          recoveredDocOpens
            .find(_.docId.contains(docId))
            .filter(_.id.nonEmpty)
            .map(_.id)
        }.headOption

        possibleId match {
          case Some(id) => cs.copy(id = id)
          case None     => cs
        }
      case cs => cs
    }

    session.copy(
      docOpens = recoveredDocOpens,
      quickSearches = recoveredQuickSearches,
      cardSearches = recoveredCardSearches
    )
  }
}
