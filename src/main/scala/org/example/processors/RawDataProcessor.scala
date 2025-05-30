package org.example.processors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.example.errorProcessors.Errors
import org.example.events.Session

object RawDataProcessor {

  case class ParseContext(
      filePath: String,
      iterator: BufferedIterator[String],
      var currentSession: SessionBuilder,
      errorAccumulator: Errors
  )

  def process(
      spark: SparkSession,
      filePath: String,
      errorAccumulator: Errors
  ): RDD[Session] = {

    val parsed = spark.sparkContext
      .wholeTextFiles(filePath)
      .flatMap { case (filePath, content) =>
        val lines = content
          .split("\n")
          .iterator
          .buffered

        val context = ParseContext(
          filePath = filePath,
          iterator = lines,
          currentSession = SessionBuilder(filePath),
          errorAccumulator = errorAccumulator
        )

        Session.parse(context)
        context.currentSession.build()
      }
    parsed
  }
}
