package org.example
import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.example.parser.SessionParser

import java.io.File
import java.io.FileInputStream

object MainApp {
  System.setProperty("log4j.configurationFile", "log4j2.xml")
  System.setProperty("log4j2.disable.jmx", "true")

  private val configFile = new File("src/resources/log4j2.xml")
  private val source = new ConfigurationSource(new FileInputStream(configFile), configFile)
  Configurator.initialize(null, source)

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
//  logger.warn("Тест логирования - это сообщение должно появиться")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MainApp")
      .master("local[*]")
      .config("spark.ui.enabled", "false")  // Отключаем веб-интерфейс
      .config("spark.driver.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
          "-Dlog4j2.configurationFile=log4j2.xml " +
          "-Dlog4j2.disable.jmx=true " +
          "--illegal-access=deny")
      .getOrCreate()
    val sc = spark.sparkContext
    val input = sc.textFile("src/resources/data/0")
    val parsedSession = SessionParser.parse(input)

    parsedSession.take(10).foreach { session =>
      println(s"""
                 |=== Session Details ===
                 |Start: ${session.startDate.date} ${session.startDate.time}
                 |End: ${session.endDate.date} ${session.endDate.time}
                 |""".stripMargin)

      println("QS Events:")
      session.QS.zipWithIndex.foreach { case (qs, index) =>
        println(s"""
                   |  [${index + 1}] Date: ${qs.date}
                   |      Query: ${qs.query}
                   |      ID: ${qs.id}
                   |      Doc IDs: ${qs.docIds.mkString(", ")}
                   |""".stripMargin)
      }

      println("CardSearch Events:")
      session.cardSearch.zipWithIndex.foreach { case (cs, index) =>
        println(s"""
                   |  [${index + 1}] Date: ${cs.date}
                   |      Query: ${cs.query}
                   |      ID: ${cs.id}
                   |      Doc IDs: ${cs.docIds.mkString(", ")}
                   |""".stripMargin)
      }

      println("DocOpen Events:")
      session.docOpen.zipWithIndex.foreach { case (doc, index) =>
        println(s"""
                   |  [${index + 1}] Date: ${doc.date}
                   |      ID: ${doc.id}
                   |      Doc ID: ${doc.docId}
                   |""".stripMargin)
      }

      println("=" * 50)
    }
    spark.stop()
    }
}