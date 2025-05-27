package org.example.tasks

import org.apache.spark.rdd.RDD
import org.example.events.Session

import java.io.PrintWriter
import java.nio.file.Paths

object Task2 {

  def run(sessions: RDD[Session], path: String, DELIMITER: String): Unit = {
    saveResults(execute(sessions), path, DELIMITER)
  }

  private def execute(
      sessions: RDD[Session]
  ): RDD[(java.time.LocalDate, String, Int)] = {
    sessions
      .flatMap { session =>
        session.quickSearches.flatMap { qs =>
          qs.docOpens
            .filter(docOpen => docOpen.date.nonEmpty)
            .map { docOpen =>
              ((docOpen.date.get.toLocalDate, docOpen.docId), 1)
            }
        }
      }
      .reduceByKey(_ + _)
      .map { case ((date, docId), count) => (date, docId.get, count) }

  }

  private def saveResults(
      results: RDD[(java.time.LocalDate, String, Int)],
      path: String,
      DELIMITER: String
  ): Unit = {
    val writer = new PrintWriter(s"$path/task2.csv")

    try {
      writer.println(Array("date", "docId", "count").mkString(DELIMITER))

      results.collect().sortBy(-_._3).foreach { case (date, docId, count) =>
        writer.println(Array(date, docId, count).mkString(DELIMITER))
      }
    } finally {
      writer.close()
    }
  }
}
