package org.example
package parser

import org.apache.spark.rdd.RDD

import scala.collection.mutable

object SessionEventExtractor {
  def extractEvents(inputRDD: RDD[String]): RDD[String] = {
    inputRDD.mapPartitions { iter =>
      val buffer = new mutable.Queue[String]()
      val result = new mutable.ListBuffer[String]()

      for (line <- iter) {
        if (line.startsWith("SESSION_START")) {
          buffer.enqueue(line)
        } else if (line.startsWith("SESSION_END") && buffer.nonEmpty) {
          val start = buffer.dequeue()
          result += s"$start $line" // объединяем пару
        }
      }

      result.iterator
    }
  }
}