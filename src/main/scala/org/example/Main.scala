package org.example

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.example.parser.SessionParser
import org.example.events.Session
import java.io.{File, FileInputStream, PrintWriter}
import java.nio.file.{Files, Paths}

object Main {

  case class ParseError(
      filePath: String,
      line: String,
      errorType: String,
      errorMessage: String,
      eventType: String
  ) {
    def toLogString: String = {
      s"[$eventType] $errorType in $filePath: $errorMessage" +
        (if (line.nonEmpty) s"\nProblem line: $line" else "")
    }
  }

  lazy val logger: Logger = LogManager.getLogger("org.example.Main")
  private val inputPath = "src/main/resources/data"
  private val outputPath = "output/task2.csv"
  private val DELIMITER = ","

  def main(args: Array[String]): Unit = {
    initLogger()
    val spark = initSparkSession()
    val errorAccumulator = spark.sparkContext.longAccumulator("parseErrors")

    try {
      val sessions = processRawData(spark, errorAccumulator)
      val targetDocId = "ACC_45616"
      val searchCount = task1(sessions, targetDocId)

      println(
        s"\nDocument $targetDocId was searched in cards $searchCount times"
      )

      printResults(sessions, outputPath)

      logger.error(
        s"Found ${errorAccumulator.value} parsing errors. Details in logs/errors.log"
      )
    } finally {
      spark.stop()
    }
  }

  def initLogger(): Unit = {
    val configFile = new File("src/main/resources/log4j2.xml")
    System.setProperty("log4j.configurationFile", configFile.getAbsolutePath)
    System.setProperty("log4j2.disable.jmx", "true")
    val source =
      new ConfigurationSource(new FileInputStream(configFile), configFile)
    Configurator.initialize(null, source)
  }

  def initSparkSession(): SparkSession = {
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

  def processRawData(
      spark: SparkSession,
      errorAccumulator: LongAccumulator
  ): RDD[(String, Session)] = {

    val parsed = spark.sparkContext
      .wholeTextFiles(inputPath)
      .flatMap { case (filePath, content) =>
        try {
          SessionParser
            .parse(content, filePath, errorAccumulator)
            .map(session => (filePath, session))
        } catch {
          case e: Exception =>
            errorAccumulator.add(1)
            val err = ParseError(
              filePath,
              content,
              e.getClass.getSimpleName,
              e.getMessage,
              "FILE"
            )
            logger.error(err.toLogString)
            None
        }
      }
    parsed.count()
    parsed
  }

  def task1(sessions: RDD[(String, Session)], docId: String): Long = {
    val pattern = docId.toLowerCase
      .replace("a", "[аa]")
      .replace("c", "[cс]")
      .replace("e", "[eе]")
      .r

    val count = sessions
      .flatMap { case (_, session) =>
        Option(session.cardSearches)
          .getOrElse(Seq.empty)
          .map(_.query)
      }
      .filter { query =>
        pattern.findFirstIn(query.toLowerCase).isDefined
      }
      .count()

    count
  }

  def task2(
      sessions: RDD[(String, Session)]
  ): Seq[(String, String, Int)] = {
    val docIds = sessions
      .flatMap { case (_, session) =>
        session.quickSearches
          .filter(_.id != 0)
          .flatMap(_.docIds)
      }
      .collect()
      .toSet

    sessions
      .flatMap { case (_, session) =>
        session.docOpens
          .filter(docOpen => docIds.contains(docOpen.docId))
          .map(docOpen => ((docOpen.date.date, docOpen.docId), 1))
      }
      .reduceByKey(_ + _)
      .map { case ((date, docId), count) => (date, docId, count) }
      .collect()
      .toSeq
      .sortBy(-_._3)
  }

  def printResults(
      sessions: RDD[(String, Session)],
      path: String
  ): Unit = {
    val stats = task2(sessions)

    println(s"\nDate${DELIMITER}DocID${DELIMITER}Count")
    println("\n------------------")
    stats.take(5).foreach { case (date, docId, count) =>
      println(f"$date$DELIMITER$docId$DELIMITER$count")
    }

    saveResults(stats, path)
  }

  def saveResults(
      stats: Seq[(String, String, Int)],
      filename: String
  ): Unit = {
    val path = Paths.get(filename)
    val writer = new PrintWriter(Files.newBufferedWriter(path))

    try {
      writer.println(s"date${DELIMITER}docId${DELIMITER}count")

      stats.foreach { case (date, docId, count) =>
        writer.println(s"${date.toString}$DELIMITER$docId$DELIMITER$count")
      }

      println(s"\nResults are saved to file: ${path.toAbsolutePath}")
    } finally {
      writer.close()
    }
  }
}
