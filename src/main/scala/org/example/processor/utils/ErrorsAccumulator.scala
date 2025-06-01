package org.example.processor.utils

import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.mapAsScalaConcurrentMapConverter

case class ErrorsAccumulator()
    extends AccumulatorV2[ParseError, Map[(String, String, String), Int]] {

  private val counts = new ConcurrentHashMap[(String, String, String), AtomicInteger]()

  override def isZero: Boolean = counts.isEmpty

  override def copy(): ErrorsAccumulator = {
    val acc = new ErrorsAccumulator
    counts.forEach { (k, v) =>
      acc.counts.put(k, new AtomicInteger(v.get))
    }
    acc
  }

  override def reset(): Unit = counts.clear()

  override def add(err: ParseError): Unit = {
    val key = (err.filePath, err.methodName, err.errorType)
    counts.computeIfAbsent(key, _ => new AtomicInteger(0)).incrementAndGet()
  }

  override def merge(
      other: AccumulatorV2[ParseError, Map[(String, String, String), Int]]
  ): Unit = {
    other.value.foreach { case (k, v) =>
      counts.computeIfAbsent(k, _ => new AtomicInteger(0)).addAndGet(v)
    }
  }

  override def value: Map[(String, String, String), Int] = {
    counts.asScala.map { case (k, v) => (k, v.get) }.toMap
  }
}
