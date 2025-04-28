package org.example
import org.apache.spark.rdd.RDD

object SessionEventExtractor {
  def extractEvents(inputRDD: RDD[String]): RDD[String] = {
    // 1. Собираем индексы всех стартовых событий
    val startIndices = inputRDD.zipWithIndex()
      .filter { case (line, _) => line.startsWith("SESSION_START") }
      .map { case (_, idx) => idx }
      .collect()
      .toSet

    // 2. Собираем индексы всех конечных событий
    val endIndices = inputRDD.zipWithIndex()
      .filter { case (line, _) => line.startsWith("SESSION_END") }
      .map { case (_, idx) => idx }
      .collect()
      .toSet

    // 3. Сопоставляем каждому START ближайший следующий END
    val sessionPairs = startIndices.flatMap { startIdx =>
      endIndices.find(endIdx => endIdx > startIdx).map(endIdx => (startIdx, endIdx))
    }

    // 4. Извлекаем полные пары
    inputRDD.zipWithIndex()
      .filter { case (_, idx) =>
        sessionPairs.exists(p => p._1 == idx || p._2 == idx)
      }
      .map { case (line, idx) =>
        sessionPairs.find(p => p._1 == idx || p._2 == idx).get -> line
      }
      .groupByKey()
      .map { case ((startIdx, endIdx), lines) =>
        val sortedLines = lines.toSeq.sortBy(line =>
          if (line.startsWith("SESSION_START")) 0 else 1)
        sortedLines.mkString(" ")
      }
  }
}
