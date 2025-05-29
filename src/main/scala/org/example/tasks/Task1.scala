package org.example.tasks

import org.apache.spark.rdd.RDD
import org.example.events.Session

import java.io.PrintWriter

case class Task1(targetDocId: String, searchCount: Int)

object Task1 {

  def run(
      sessions: RDD[Session],
      targetDocId: String = "ACC_45616",
      outputPath: String = "output"
  ): Unit = {
    val result = execute(sessions, targetDocId)
    val writer = new PrintWriter(s"$outputPath/task1.log")
    try {
      writer.println(
        s"\nDocument ${result.targetDocId} was searched in cards ${result.searchCount} times"
      )
    } finally {
      writer.close()
    }
  }

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
