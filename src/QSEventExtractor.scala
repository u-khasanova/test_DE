package org.example
import org.apache.spark.rdd.RDD

object QSEventExtractor {
  def extractEvents(inputRDD: RDD[String]): RDD[String] = {
    // find QS-lines indices
    val qsIndices = inputRDD.zipWithIndex()
      .filter { case (line, _) => line.startsWith("QS") }
      .map { case (_, idx) => idx }
      .collect()
      .toSet

    // join QS-lines with next line
    inputRDD.zipWithIndex()
      .filter { case (_, idx) => qsIndices.contains(idx) || qsIndices.contains(idx - 1) }
      .map { case (line, idx) => (if (qsIndices.contains(idx)) idx else idx - 1, line) }
      .groupByKey()
      .map { case (_, lines) => lines.mkString(" ") }
  }
}