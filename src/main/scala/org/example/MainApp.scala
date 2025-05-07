package org.example

import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}
import org.apache.spark.sql.SparkSession
import org.example.parser.SessionParser
import org.example.events.Session
import java.io.{File, FileInputStream, PrintWriter}
import java.nio.file.Paths

object MainApp {
  // Logger configuration
  private def logInit(): Unit = {
    System.setProperty("log4j.configurationFile", "log4j2.xml")
    System.setProperty("log4j2.disable.jmx", "true")

    val configFile = new File("src/resources/log4j2.xml")
    val source = new ConfigurationSource(new FileInputStream(configFile), configFile)
    Configurator.initialize(null, source)
  }

  // Spark context initialization
  private def sparkContextInit(): SparkSession = {
    val javaOptions = Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "-Dlog4j2.configurationFile=log4j2.xml",
      "-Dlog4j2.disable.jmx=true",
      "--illegal-access=deny"
    ).mkString(" ")

    SparkSession.builder()
      .appName("SessionProcessor")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.extraJavaOptions", javaOptions)
      .getOrCreate()
  }


  // Counting document searches in CardSearch
  private def countDocumentSearches(sessions: Seq[(String, Session)], docId: String): Long = {
    sessions.flatMap { case (_, session) =>
      session.cardSearches.flatMap(_.docIds)
    }.count(_ == docId)
  }

  // Collecting stats on document openings by day
  private def collectDocOpenStats(sessions: Seq[(String, Session)]): Map[(String, String), Int] = {
    sessions.flatMap { case (_, session) =>
        val day = session.startDate.date
        session.docOpens.map(doc => (day, doc.docId) -> 1)
      }.groupBy(_._1)
      .mapValues(_.map(_._2).sum)
  }

  // Writing stats to a CSV file
  private def writeStatsToCsv(stats: Map[(String, String), Int], outputPath: String): Unit = {
    val writer = new PrintWriter(outputPath)
    try {
      writer.println("date,doc_id,open_count") // Заголовок CSV
      stats.toSeq
        .sortBy { case ((date, _), _) => date }
        .foreach { case ((date, docId), count) =>
          writer.println(s"$date,$docId,$count")
        }
    } finally {
      writer.close()
    }
  }

  // File processing and session collection
  private def processFiles(spark: SparkSession, filePath: String): Seq[(String, Session)] = {
    spark.sparkContext
      .wholeTextFiles(filePath)
      .flatMap { case (filePath, content) =>
        SessionParser.parse(content).map(session => (filePath, session))
      }
      .collect()
  }

  def main(args: Array[String]): Unit = {
    logInit()
    val spark = sparkContextInit()

    try {
      val inputPath = "src/resources/data"
      val outputPath = "output/doc_open_stats.csv"

      val sessions = processFiles(spark, inputPath)

      // 1. Counting searches for a specific document
      val targetDocId = "ACC_45616"
      val searchCount = countDocumentSearches(sessions, targetDocId)
      println(s"\nDocument $targetDocId was searched in cards $searchCount times")

      // 2. Collecting and recording discovery statistics
      val docOpenStats = collectDocOpenStats(sessions)
      writeStatsToCsv(docOpenStats, outputPath)
      println(s"\nOpening statistics are saved in: ${Paths.get(outputPath).toAbsolutePath}")

      // Example of displaying the first 5 records
      println("\nFirst 5 statistics records:")
      docOpenStats.toSeq
        .sortBy { case ((date, _), _) => date }
        .take(5)
        .foreach { case ((date, docId), count) =>
          println(f"$date%-12s $docId%-15s $count%5d")
        }

    } finally {
      spark.stop()
    }
  }
}