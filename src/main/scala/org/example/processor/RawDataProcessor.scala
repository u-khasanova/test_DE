package org.example.processor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.example.processor.events.Session
import org.example.processor.utils.{ErrorsAccumulator, ParseContext}

object RawDataProcessor {

  def process(
      spark: SparkSession,
      filePath: String,
      errorAccumulator: ErrorsAccumulator
  ): RDD[Session] = {

    val parsed = spark.sparkContext
      .wholeTextFiles(filePath)
      .map { case (filePath, content) =>
        val lines = content
          .split("\n")
          .iterator
          .buffered

        val context = ParseContext(
          filePath = filePath,
          iterator = lines,
          currentSession = SessionBuilder(filePath)
        )

        val session = Session.parse(context)
        context.errors.foreach(errorAccumulator.add)
        session
      }
    parsed
  }
}
