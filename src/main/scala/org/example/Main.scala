package org.example

import org.example.processor.RawDataProcessor
import org.example.processor.utils.{ErrorsAccumulator, SparkSessionInitializer}
import org.example.tasks.{Task1, Task2}
import org.example.utils.LoggerInitializer

import java.io.PrintWriter

object Main {

  private val inputPath = "src/main/resources/data"

  def main(args: Array[String]): Unit = {
    LoggerInitializer.init()
    val spark = SparkSessionInitializer.init()

    val errorAccumulator = new ErrorsAccumulator
    spark.sparkContext.register(errorAccumulator, "parseErrors")

    try {
      val sessions = RawDataProcessor.process(spark, inputPath, errorAccumulator)

      Task1.run(sessions)
      Task2.run(sessions)

      val errors = errorAccumulator.value
      writeErrors(errors)

    } finally {
      spark.stop()
    }
  }

  private def writeErrors(errors: Map[(String, String, String), Int]): Unit = {
    val writer = new PrintWriter("logs/errors.log")
    try {
      for (((filePath, method, errorType), count) <- errors) {
        writer.println(
          s"$errorType in $method: $count time(s)\nFile: $filePath\n"
        )
      }
    } finally {
      writer.close()
    }
  }
}
