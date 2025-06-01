package org.example.tasks

import org.apache.spark.rdd.RDD
import org.example.processor.events.Session

import java.io.PrintWriter

object Task2 {

  private val path = "output"
  private val delimiter = ","

  def run(sessions: RDD[Session]): Unit = {
    saveResults(countData(sessions), path, delimiter)
  }

  private def countData(sessions: RDD[Session]): RDD[(String, String, Int)] = {
    sessions
      .flatMap { session =>
        session.quickSearches.flatMap { qs =>
          qs.docOpens.map { docOpen =>
            val date = docOpen.date
              .map(_.toLocalDate.toString)
              .getOrElse("empty date")

            ((date, docOpen.docId), 1)
          }
        }
      }
      .reduceByKey(_ + _)
      .map { case ((date, docId), count) => (date, docId.get, count) }

  }

  private def saveResults(
      results: RDD[(String, String, Int)],
      path: String,
      delimiter: String
  ): Unit = {

    val writer = new PrintWriter(s"$path/task2.csv")

    try {
      writer.println(Array("date", "docId", "count").mkString(delimiter))

      results.collect().sortBy(-_._3).foreach { case (date, docId, count) =>
        writer.println(Array(date, docId, count).mkString(delimiter))
      }
    } finally {
      writer.close()
    }
  }
}
