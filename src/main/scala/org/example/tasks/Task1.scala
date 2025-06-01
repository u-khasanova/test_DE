package org.example.tasks

import org.apache.spark.rdd.RDD
import org.example.processor.events.Session

import java.io.PrintWriter

case class Task1(targetDocId: String, searchCount: Int)

object Task1 {

  private val targetDocId = "ACC_45616"

  private val outputPath = "output"

  def run(
      sessions: RDD[Session]
  ): Unit = {

    val result = countData(sessions, targetDocId)

    val writer = new PrintWriter(s"$outputPath/task1.log")

    try {
      writer.println(
        s"\nDocument $targetDocId was searched in cards $result times"
      )
    } finally {
      writer.close()
    }

  }

  private def countData(sessions: RDD[Session], targetDocId: String): Int = {
    val pattern = targetDocId
      .toLowerCase()
      .replace("a", "[aа]")
      .replace("c", "[cс]")
      .replace("e", "[eе]")
      .r

    val searchCount = sessions
      .flatMap(_.cardSearches.flatMap(_.query))
      .map { case (_, value) => value }
      .filter(value => pattern.findFirstIn(value.toLowerCase).isDefined)
      .count()
      .toInt

    searchCount
  }
}
