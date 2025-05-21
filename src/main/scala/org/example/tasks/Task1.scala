package org.example.tasks

import org.apache.spark.rdd.RDD
import org.example.events.Session

case class Task1(targetDocId: String, searchCount: Int)

object Task1 {
  def execute(sessions: RDD[Session], targetDocId: String): Task1 = {
    val pattern = targetDocId
      .toLowerCase()
      .replace("a", "[аa]")
      .replace("c", "[cс]")
      .replace("e", "[eе]")
      .r

    val searchCount = sessions
      .flatMap { session =>
        session.cardSearches.map(_.query)
      }
      .filter { query =>
        pattern.findFirstIn(query.toLowerCase).isDefined
      }
      .count()
      .toInt

    Task1(targetDocId, searchCount)
  }
}
