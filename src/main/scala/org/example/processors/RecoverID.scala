package org.example.processors

import org.example.events.{CardSearch, DocOpen, QuickSearch, Session}

import scala.collection.mutable

object RecoverID {
  def recoverIds(session: Session): Session = {

    /*
    Step 1: recover DocOpen ids based on quick / card search ids

    DocOpen must inherit id from parent search if DocOpen.docId:
     - belongs only to QuickSearch.docIds
    OR
     - belongs only to CardSearch.docIds
     */

    val docIdToSearchId = mutable.Map[String, Int]()
    val ambiguousDocs = mutable.Set[String]()

    (session.quickSearches ++ session.cardSearches).foreach {
      case qs: QuickSearch if qs.id != 0 =>
        qs.docIds.foreach { docId =>
          if (docIdToSearchId.contains(docId)) ambiguousDocs.add(docId)
          else docIdToSearchId.put(docId, qs.id)
        }
      case cs: CardSearch if cs.id != 0 =>
        cs.docIds.foreach { docId =>
          if (docIdToSearchId.contains(docId)) ambiguousDocs.add(docId)
          else docIdToSearchId.put(docId, cs.id)
        }
      case _ =>
    }

    val recoveredDocOpens = session.docOpens.map { docOpen =>
      if (docOpen.id == 0) {
        docIdToSearchId.get(docOpen.docId) match {
          case Some(id) if !ambiguousDocs(docOpen.docId) =>
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
      if (docOpen.id != 0) {
        val currentDocs = searchIdToDocIds.getOrElse(docOpen.id, Set.empty)
        searchIdToDocIds.put(docOpen.id, currentDocs + docOpen.docId)
      }
    }

    val recoveredQuickSearches = session.quickSearches.map {
      case qs: QuickSearch if qs.id == 0 =>
        val possibleId = qs.docIds.flatMap { docId =>
          recoveredDocOpens.find(_.docId == docId).filter(_.id != 0).map(_.id)
        }.headOption

        possibleId match {
          case Some(id) => qs.copy(id = id)
          case None     => qs
        }
      case qs => qs
    }

    val recoveredCardSearches = session.cardSearches.map {
      case cs: CardSearch if cs.id == 0 =>
        val possibleId = cs.docIds.flatMap { docId =>
          recoveredDocOpens.find(_.docId == docId).filter(_.id != 0).map(_.id)
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
