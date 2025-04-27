package org.example
import org.apache.spark.rdd.RDD

object DOEventExtractor {
  def extractEvents(inputRDD: RDD[String]): RDD[String] = {
    // find QS-lines indices
    val doIndices = inputRDD.zipWithIndex()
      .filter { case (line, _) => line.startsWith("DOC_OPEN") }
      .map { case (_, idx) => idx }
      .collect()
      .toSet

    // return DOC_OPEN lines
    inputRDD.zipWithIndex()
      .filter { case (_, idx) => doIndices.contains(idx)}
      .map { case (line, idx) => (idx, line) }
      .groupByKey()
      .map { case (_, lines) => lines.mkString(" ") }
  }
}