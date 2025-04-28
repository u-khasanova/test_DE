package org.example
import org.apache.spark.rdd.RDD

object QSEventExtractor {
  def extractEvents(inputRDD: RDD[String]): RDD[String] = {
    inputRDD.mapPartitions { iter =>
      iter.sliding(2).collect {
        case Seq(qsLine, nextLine) if qsLine.startsWith("QS") => s"$qsLine $nextLine"
      }
    }
  }
}