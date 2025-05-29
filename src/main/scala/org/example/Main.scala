package org.example

import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.example.errorProcessors.ErrorAccumulator
import org.example.events.Session
import org.example.processors.RawDataProcessor
import org.example.tasks.{Task1, Task2}

import java.io.{File, FileInputStream, PrintWriter}

object Main {

  private val inputPath = "src/main/resources/data"
  private val recoverId = false
  private val recoverEmptyDate = false

  def main(args: Array[String]): Unit = {
    initLogger()
    val spark = initSparkSession()

    val errorAccumulator = new ErrorAccumulator
    spark.sparkContext.register(errorAccumulator, "parseErrors")

    try {
      val sessions =
        processRawData(spark, errorAccumulator, recoverId, recoverEmptyDate)

      Task1.run(sessions)
      Task2.run(sessions)

      val writer = new PrintWriter("logs/errors.log")
      try {
        for (((method, errorType), count) <- errorAccumulator.value) {
          writer.println(s"$errorType in $method: $count time(s)")
        }
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
      errorAccumulator: ErrorAccumulator,
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
