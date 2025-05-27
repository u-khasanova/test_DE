package org.example

import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.CollectionAccumulator
import org.example.events.Session
import org.example.processors.RawDataProcessor
import org.example.tasks.{Task1, Task2}

import java.io.{File, FileInputStream, PrintWriter}

object Main {

  case class ParseError(
      filePath: String,
      line: String,
      methodName: String,
      errorType: String,
      errorMessage: String
  ) {
    def toLogString: String = {
      s"$errorType in $filePath using $methodName: $errorMessage" +
        (if (line.nonEmpty) s"\nProblem line: $line" else "")
    }
  }

  private val inputPath = "src/main/resources/data"
  private val outputPath = "output"
  private val DELIMITER = ","
  private val targetDocId = "ACC_45616"

  private val recoverId = true
  private val recoverEmptyDate = true

  def main(args: Array[String]): Unit = {
    initLogger()
    val spark = initSparkSession()
    val errorAccumulator =
      spark.sparkContext.collectionAccumulator[String]("parseErrors")

    try {
      val sessions =
        processRawData(spark, errorAccumulator, recoverId, recoverEmptyDate)

      Task1.run(sessions, targetDocId, outputPath)
      Task2.run(sessions, outputPath, DELIMITER)

      val writer = new PrintWriter("logs/errors.log")
      try {
        errorAccumulator.value.forEach(writer.println)
      } finally {
        writer.close()
      }
    } finally {
      spark.stop()
    }
  }

  private def initLogger(): Unit = {
    val configFile = new File("src/main/resources/log4j2.xml")
    System.setProperty("log4j.configurationFile", configFile.getAbsolutePath)
    System.setProperty("log4j2.disable.jmx", "true")
    val source =
      new ConfigurationSource(new FileInputStream(configFile), configFile)
    Configurator.initialize(null, source)
  }

  private def initSparkSession(): SparkSession = {
    val javaOptions = Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "-Dlog4j2.configurationFile=log4j2.xml",
      "-Dlog4j2.disable.jmx=true",
      "--illegal-access=deny"
    ).mkString(" ")

    SparkSession
      .builder()
      .appName("SessionProcessor")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.extraJavaOptions", javaOptions)
      .getOrCreate()
  }

  private def processRawData(
      spark: SparkSession,
      errorAccumulator: CollectionAccumulator[String],
      recoverId: Boolean,
      processEmptyDate: Boolean
  ): RDD[Session] = {

    val parsed = spark.sparkContext
      .wholeTextFiles(inputPath)
      .flatMap { case (filePath, content) =>
        RawDataProcessor
          .process(
            content,
            filePath,
            errorAccumulator,
            recoverId,
            processEmptyDate
          )
      }
    parsed
  }
}
