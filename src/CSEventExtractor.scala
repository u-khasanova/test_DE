package org.example
import org.apache.spark.rdd.RDD

object CSEventExtractor {
  def extractEvents(inputRDD: RDD[String]): RDD[String] = {
    // Собираем все строки с индексами
    val linesWithIndex = inputRDD.zipWithIndex().collect()

    // Находим пары (startIndex, endIndex) для каждого события
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

    // Собираем события в нужном формате
    val events = eventRanges.map { case (startIdx, endIdx) =>
      // Получаем все строки события (от START до END включительно)
      val eventLines = (startIdx to endIdx + 1).map(i =>
        linesWithIndex.find(_._2 == i).map(_._1).getOrElse(""))

      // Разбираем компоненты
      val startLine = eventLines.head
      val queryLines = eventLines.tail.init // Все строки между START и END
      val endLine = eventLines.last

      // Извлекаем результаты из строки после CARD_SEARCH_END
      val searchResults = endLine.replace("CARD_SEARCH_END", "").trim

      // Формируем итоговую строку
      s"$startLine ${queryLines.mkString(" ")} $searchResults".replaceAll("\\s+", " ").trim
    }

    inputRDD.sparkContext.parallelize(events)
  }
}