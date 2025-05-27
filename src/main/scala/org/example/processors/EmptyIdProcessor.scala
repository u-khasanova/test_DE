package org.example.processors

import org.example.events.{CardSearch, QuickSearch, Session}

import scala.collection.mutable

object EmptyIdProcessor {
  def recover(session: Session): Session = {
    val docIdToSearches = mutable.Map[String, List[Option[Int]]]()

    (session.quickSearches ++ session.cardSearches).foreach {
      case qs: QuickSearch =>
        qs.docIds.foreach { docId =>
          docIdToSearches.update(
            docId,
            qs.id :: docIdToSearches.getOrElse(docId, Nil)
          )
        }
      case cs: CardSearch =>
        cs.docIds.foreach { docId =>
          docIdToSearches.update(
            docId,
            cs.id :: docIdToSearches.getOrElse(docId, Nil)
          )
        }
    }

    /*
        Step 1: recover DocOpen ids based on quick / card search ids

        DocOpen must inherit id from parent search if DocOpen.docId:
         - belongs only to QuickSearch.docIds
        OR
         - belongs only to CardSearch.docIds
     */

    val recoveredDocOpens = session.docOpens.map { docOpen =>
      docOpen.docId match {
        case Some(docId) if docOpen.id.isEmpty || docOpen.id.contains(0) =>
          docIdToSearches.get(docId) match {
            case Some(List(searchId)) =>
              searchId match {
                case Some(id) => docOpen.copy(id = Some(id))
                case None     => docOpen
              }
            case _ => docOpen
          }
        case _ => docOpen
      }
    }

    /*
        Step 2: recover quick / card search

        Update search's id if at least one document was opened only within given search
     */

    val recoveredQuickSearches = session.quickSearches.map { qs =>
      if (qs.id.isEmpty) {
        val uniqueDocId = qs.docIds.find { docId =>
          docIdToSearches.get(docId) match {
            case Some(List(searchId)) =>
              searchId.isEmpty
            case _ => false
          }
        }

        uniqueDocId.flatMap { docId =>
          recoveredDocOpens.find(_.docId.contains(docId)).flatMap(_.id)
        } match {
          case Some(id) => qs.copy(id = Some(id))
          case None     => qs
        }
      } else {
        qs
      }
    }

    val recoveredCardSearches = session.cardSearches.map { cs =>
      if (cs.id.isEmpty) {
        val uniqueDocId = cs.docIds.find { docId =>
          docIdToSearches.get(docId) match {
            case Some(List(searchId)) => searchId.isEmpty
            case _                    => false
          }
        }

        uniqueDocId.flatMap { docId =>
          recoveredDocOpens.find(_.docId.contains(docId)).flatMap(_.id)
        } match {
          case Some(id) => cs.copy(id = Some(id))
          case None     => cs
        }
      } else {
        cs
      }
    }

    session.copy(
      docOpens = recoveredDocOpens,
      quickSearches = recoveredQuickSearches,
      cardSearches = recoveredCardSearches
    )
  }
}
