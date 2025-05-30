package org.example.processors.fixers

import org.example.events.Session

import scala.collection.mutable

object EmptyIdFixer {
  def recover(session: Session): Session = {
    val docIdToSearches = mutable.Map[String, List[Option[Int]]]()

    session.quickSearches.foreach { qs =>
      qs.docIds.foreach { docId =>
        docIdToSearches.update(
          docId,
          qs.searchId :: docIdToSearches.getOrElse(docId, Nil)
        )
      }
    }

    session.cardSearches.foreach { cs =>
      cs.docIds.foreach { docId =>
        docIdToSearches.update(
          docId,
          cs.searchId :: docIdToSearches.getOrElse(docId, Nil)
        )
      }
    }

    session.docOpens.foreach { docOpen =>
      if (docOpen.searchId.isEmpty && docOpen.docId.isDefined) {
        docIdToSearches.get(docOpen.docId.get) match {
          case Some(List(Some(id))) => docOpen.searchId = Some(id)
          case _                    =>
        }
      }
    }

    session.quickSearches.foreach { qs =>
      if (qs.searchId.isEmpty) {
        qs.docIds
          .find { docId =>
            docIdToSearches.get(docId) match {
              case Some(List(None)) => true
              case _                => false
            }
          }
          .flatMap { docId =>
            session.docOpens.find(_.docId.contains(docId)).flatMap(_.searchId)
          }
          .foreach { id =>
            qs.searchId = Some(id)
          }
      }
    }

    session.cardSearches.foreach { cs =>
      if (cs.searchId.isEmpty) {
        cs.docIds
          .find { docId =>
            docIdToSearches.get(docId) match {
              case Some(List(None)) => true
              case _                => false
            }
          }
          .flatMap { docId =>
            session.docOpens.find(_.docId.contains(docId)).flatMap(_.searchId)
          }
          .foreach { id =>
            cs.searchId = Some(id)
          }
      }
    }

    session
  }
}
