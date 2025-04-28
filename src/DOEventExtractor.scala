package org.example
import org.apache.spark.rdd.RDD

object DOEventExtractor {
  def extractEvents(inputRDD: RDD[String]): RDD[String] = {
    inputRDD.mapPartitions { iter =>
      iter.sliding(1).collect {
        case Seq(doLine) if doLine.startsWith("DOC_OPEN") => s"$doLine"
      }
    }
  }
}