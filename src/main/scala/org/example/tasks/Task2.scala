package org.example.tasks

import org.apache.spark.rdd.RDD
import org.example.events.Session
import java.nio.file.{Files, Paths}
import java.io.PrintWriter

object Task2 {

  def execute(
      sessions: RDD[Session]
  ): RDD[(String, String, Int)] = {
    sessions
      .flatMap { session =>
        session.quickSearches.flatMap { qs =>
          qs.docOpens
            .map { docOpen =>
              ((docOpen.date.get.date, docOpen.docId), 1)
            }
        }
      }
      .reduceByKey(_ + _)
      .map { case ((date, docId), count) => (date, docId.get, count) }
  }

  def saveResults(
      results: RDD[(String, String, Int)],
      path: String,
      DELIMITER: String
  ): Unit = {
    val outputPath = Paths.get(path)
    val writer = new PrintWriter(Files.newBufferedWriter(outputPath))

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
