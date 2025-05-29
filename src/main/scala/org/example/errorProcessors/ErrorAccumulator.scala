package org.example.errorProcessors

import org.apache.spark.util.AccumulatorV2

import scala.collection.compat.toMutableMapExtensionMethods

case class ErrorAccumulator()
    extends AccumulatorV2[ParseError, Map[(String, String), Int]] {

  private val counts = scala.collection.mutable.Map[(String, String), Int]()

  override def isZero: Boolean = counts.isEmpty
  override def copy(): ErrorAccumulator = {
    val acc = new ErrorAccumulator
    counts.synchronized { acc.counts ++= counts }
    acc
  }
  override def reset(): Unit = counts.clear()

  override def add(err: ParseError): Unit = {
    val key = (err.methodName, err.errorType)
    counts.synchronized {
      counts.updateWith(key) {
        case Some(c) => Some(c + 1)
        case None    => Some(1)
      }
    }
  }

  override def merge(
      other: AccumulatorV2[ParseError, Map[(String, String), Int]]
  ): Unit = {
    other.value.foreach { case (k, v) =>
      counts.updateWith(k) {
        case Some(c) => Some(c + v)
        case None    => Some(v)
      }
    }
  }

  override def value: Map[(String, String), Int] = counts.toMap
}
