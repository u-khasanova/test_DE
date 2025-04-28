package org.example
import org.apache.spark.rdd.RDD

object CSEventExtractor {
  def extractEvents(inputRDD: RDD[String]): RDD[String] = {
    val linesWithIndex = inputRDD.zipWithIndex().collect()

    val eventRanges = {
      var currentStart: Option[Long] = None
      linesWithIndex.flatMap { case (line, idx) =>
        if (line.startsWith("CARD_SEARCH_START")) {
          currentStart = Some(idx)
          None
        } else if (line.startsWith("CARD_SEARCH_END") && currentStart.isDefined) {
          val range = (currentStart.get, idx)
          currentStart = None
          Some(range)
        } else {
          None
        }
      }
    }

    val events = eventRanges.map { case (startIdx, endIdx) =>
      val eventLines = (startIdx to endIdx + 1).map(i =>
        linesWithIndex.find(_._2 == i).map(_._1).getOrElse(""))

      val startLine = eventLines.head
      val queryLines = eventLines.tail.init
      val endLine = eventLines.last

      val searchResults = endLine.replace("CARD_SEARCH_END", "").trim

      s"$startLine ${queryLines.mkString(" ")} $searchResults".replaceAll("\\s+", " ").trim
    }

    inputRDD.sparkContext.parallelize(events)
  }
}